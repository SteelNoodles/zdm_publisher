"""主程序入口"""
import asyncio
import logging
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lx.config.config_manager import ConfigManager
from src.lx.service.crawler_service import CrawlerService
from src.lx.service.data_processing_service import DataProcessingService
from src.lx.service.notification_service import NotificationService
from src.lx.utils.db_util import DatabaseUtil


def setup_logging(log_level: str = 'INFO'):
    """设置日志配置"""
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('zdm_crawler.log', encoding='utf-8')
        ]
    )


async def main():
    """主函数"""
    # 初始化配置管理器
    config_manager = ConfigManager()
    
    # 设置日志级别
    setup_logging(config_manager.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("===== 什么值得买优惠信息推送工具 启动 =====")
    
    # 验证配置
    if not config_manager.validate_config():
        logger.error("配置无效，程序退出")
        return False
    
    # 打印配置摘要
    config_manager.print_config_summary()
    
    # 初始化各个服务
    db_util = DatabaseUtil(config_manager.db_path)
    crawler_service = None
    
    # 初始化爬虫服务 - 添加错误处理
    try:
        crawler_service = CrawlerService()
        logger.info("爬虫服务初始化成功")
    except Exception as e:
        logger.error(f"爬虫服务初始化失败: {str(e)}")
        try:
            await db_util.disconnect()
        except:
            pass
        return False
    
    data_processing_service = DataProcessingService()
    notification_service = NotificationService()
    
    # 设置数据处理服务的过滤阈值
    data_processing_service.set_thresholds(
        min_vote=config_manager.min_vote_threshold,
        min_comment=config_manager.min_comment_threshold
    )
    
    # 设置推送服务配置
    if config_manager.use_email:
        notification_service.set_email_config(
            host=config_manager.email_host,
            port=config_manager.email_port,
            username=config_manager.email_username,
            password=config_manager.email_password,
            from_addr=config_manager.email_from,
            to_addrs=config_manager.email_to
        )
    
    if config_manager.use_wechat:
        notification_service.set_wx_config(
            app_token=config_manager.wx_app_token,
            topic_ids=config_manager.wx_topic_ids,
            uids=config_manager.wx_uids
        )
    
    try:
        # 1. 获取已推送的商品ID
        logger.info("获取已推送商品ID...")
        pushed_ids = await db_util.get_pushed_ids()
        logger.info(f"已推送商品数量: {len(pushed_ids)}")
        
        # 2. 爬取什么值得买数据 - 添加错误处理
        logger.info("开始爬取什么值得买数据...")
        raw_items = []
        try:
            raw_items = await crawler_service.crawl_zdm_data()
            logger.info(f"爬取到商品数量: {len(raw_items)}")
        except Exception as e:
            logger.error(f"爬取数据失败: {str(e)}")
        
        # 如果没有爬取到数据，尝试清除cookie后重试 - 添加错误处理
        if not raw_items:
            logger.warning("未爬取到数据，尝试清除cookie后重试...")
            try:
                crawler_service.clear_cookies()
                raw_items = await crawler_service.crawl_zdm_data()
                logger.info(f"重试后爬取到商品数量: {len(raw_items)}")
            except Exception as e:
                logger.error(f"重试爬取数据失败: {str(e)}")
        
        # 如果仍然没有数据，结束本次运行
        if not raw_items:
            logger.warning("无法获取数据，结束本次运行")
            logger.info("===== 什么值得买优惠信息推送工具 执行完成 =====")
            return True
        
        # 3. 处理和过滤数据
        logger.info("处理和过滤数据...")
        filtered_items = await data_processing_service.process_data(raw_items, pushed_ids)
        logger.info(f"过滤后符合条件的商品数量: {len(filtered_items)}")
        
        # 4. 保存数据到数据库
        if filtered_items:
            logger.info("保存数据到数据库...")
            try:
                await db_util.save_or_update_batch(filtered_items)
            except Exception as e:
                logger.error(f"保存数据失败: {str(e)}")
        
        # 5. 推送通知
        if filtered_items:
            logger.info("推送通知...")
            try:
                results = await notification_service.push_notifications(
                    filtered_items,
                    use_email=config_manager.use_email,
                    use_wechat=config_manager.use_wechat
                )
                
                # 检查推送结果，更新推送状态
                all_success = all(results.values()) if results else False
                if all_success:
                    logger.info("所有推送任务完成")
                    # 更新数据库中的推送状态
                    try:
                        article_ids = [item['article_id'] for item in filtered_items]
                        await db_util.update_pushed_status(article_ids, pushed=True)
                    except Exception as e:
                        logger.error(f"更新推送状态失败: {str(e)}")
                else:
                    logger.warning("部分推送任务失败")
                    for channel, success in results.items():
                        if not success:
                            logger.warning(f"{channel} 推送失败")
            except Exception as e:
                logger.error(f"推送通知失败: {str(e)}")
        else:
            logger.info("没有符合条件的商品需要推送")
        
        logger.info("===== 什么值得买优惠信息推送工具 执行完成 =====")
        return True
    
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        return False
    except Exception as e:
        logger.error(f"程序执行异常: {str(e)}", exc_info=True)
        return False
    finally:
        # 断开数据库连接
        try:
            await db_util.disconnect()
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {str(e)}")


if __name__ == '__main__':
    # 运行主函数
    success = asyncio.run(main())
    # 根据执行结果设置退出码
    sys.exit(0 if success else 1)