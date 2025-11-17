"""配置管理模块"""
import os
import logging
from typing import Optional, List
from dotenv import load_dotenv


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, env_file: str = '.env'):
        self.logger = logging.getLogger(__name__)
        # 加载环境变量
        if os.path.exists(env_file):
            load_dotenv(env_file)
            self.logger.info(f"已从 {env_file} 加载配置")
        else:
            self.logger.warning(f"未找到配置文件 {env_file}，将使用系统环境变量")
    
    # 数据库配置
    @property
    def db_path(self) -> str:
        """数据库文件路径"""
        return os.getenv('DB_PATH', 'database.db')
    
    # 邮件配置
    @property
    def email_host(self) -> Optional[str]:
        """邮件服务器地址"""
        return os.getenv('EMAIL_HOST')
    
    @property
    def email_port(self) -> int:
        """邮件服务器端口"""
        return int(os.getenv('EMAIL_PORT', '587'))
    
    @property
    def email_username(self) -> Optional[str]:
        """邮件用户名"""
        return os.getenv('EMAIL_USERNAME')
    
    @property
    def email_password(self) -> Optional[str]:
        """邮件密码"""
        return os.getenv('EMAIL_PASSWORD')
    
    @property
    def email_from(self) -> Optional[str]:
        """发件人地址"""
        return os.getenv('EMAIL_FROM')
    
    @property
    def email_to(self) -> List[str]:
        """收件人地址列表"""
        to_str = os.getenv('EMAIL_TO', '')
        return [email.strip() for email in to_str.split(',') if email.strip()]
    
    # 微信推送配置
    @property
    def wx_app_token(self) -> Optional[str]:
        """WxPusher的AppToken"""
        return os.getenv('WX_APP_TOKEN')
    
    @property
    def wx_topic_ids(self) -> List[int]:
        """主题ID列表"""
        topic_ids_str = os.getenv('WX_TOPIC_IDS', '')
        try:
            return [int(tid.strip()) for tid in topic_ids_str.split(',') if tid.strip()]
        except ValueError:
            self.logger.error("WX_TOPIC_IDS 格式错误，应为逗号分隔的数字列表")
            return []
    
    @property
    def wx_uids(self) -> List[str]:
        """用户ID列表"""
        uids_str = os.getenv('WX_UIDS', '')
        return [uid.strip() for uid in uids_str.split(',') if uid.strip()]
    
    # 过滤配置
    @property
    def min_vote_threshold(self) -> int:
        """最小点赞数阈值"""
        return int(os.getenv('MIN_VOTE_THRESHOLD', '10'))
    
    @property
    def min_comment_threshold(self) -> int:
        """最小评论数阈值"""
        return int(os.getenv('MIN_COMMENT_THRESHOLD', '5'))
    
    # 日志配置
    @property
    def log_level(self) -> str:
        """日志级别"""
        return os.getenv('LOG_LEVEL', 'INFO')
    
    @property
    def use_email(self) -> bool:
        """是否使用邮件推送"""
        return os.getenv('USE_EMAIL', 'True').lower() == 'true'
    
    @property
    def use_wechat(self) -> bool:
        """是否使用微信推送"""
        return os.getenv('USE_WECHAT', 'False').lower() == 'true'
    
    def validate_config(self) -> bool:
        """验证配置是否有效
        
        Returns:
            配置是否有效
        """
        # 检查推送配置至少有一个有效
        if self.use_email:
            if not all([self.email_host, self.email_username, self.email_password, self.email_from, self.email_to]):
                self.logger.error("邮件推送配置不完整")
                # 如果只配置了邮件但配置不完整，返回False
                if not self.use_wechat:
                    return False
        
        if self.use_wechat:
            if not self.wx_app_token:
                self.logger.error("微信推送配置不完整（缺少AppToken）")
                # 如果只配置了微信但配置不完整，返回False
                if not self.use_email:
                    return False
        
        # 如果两个推送都配置了但都不完整，返回False
        if self.use_email and self.use_wechat:
            has_valid_email = all([self.email_host, self.email_username, self.email_password, self.email_from, self.email_to])
            has_valid_wechat = bool(self.wx_app_token)
            if not has_valid_email and not has_valid_wechat:
                self.logger.error("邮件和微信推送配置都不完整")
                return False
        
        return True
    
    def print_config_summary(self):
        """打印配置摘要"""
        self.logger.info("=== 配置摘要 ===")
        self.logger.info(f"数据库路径: {self.db_path}")
        
        if self.use_email:
            self.logger.info(f"邮件推送: 已启用")
            self.logger.info(f"  收件人数量: {len(self.email_to)}")
        else:
            self.logger.info(f"邮件推送: 已禁用")
        
        if self.use_wechat:
            self.logger.info(f"微信推送: 已启用")
            self.logger.info(f"  主题ID数量: {len(self.wx_topic_ids)}")
            self.logger.info(f"  用户ID数量: {len(self.wx_uids)}")
        else:
            self.logger.info(f"微信推送: 已禁用")
        
        self.logger.info(f"过滤阈值: 点赞数 ≥ {self.min_vote_threshold}, 评论数 ≥ {self.min_comment_threshold}")
        self.logger.info("================")