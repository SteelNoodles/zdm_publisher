"""爬虫服务类，负责数据抓取"""
import asyncio
import aiohttp
import json
import logging
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

from src.lx.utils.constants import ZDM_URL, MAX_RETRY
from src.lx.utils.utils import random_user_agent, str_number_format


class CrawlerService:
    """异步爬虫服务"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.cookies = None
        self.cookies_file = 'cookies.txt'
        self.cookies_last_updated = 0
        self.cookies_expire_time = 3600  # Cookie有效期1小时
    
    async def crawl_zdm_data(self) -> List[Dict]:
        """爬取什么值得买数据"""
        all_items = []
        
        try:
            # 尝试更新cookies，失败时静默处理，使用已有cookies
            try:
                await self._get_or_update_cookies()
            except Exception as e:
                self.logger.warning(f"更新Cookie时出错: {str(e)}，继续使用现有Cookie")
            
            for url in ZDM_URL:
                try:
                    items = await self._fetch_page_data(url)
                    if items:
                        all_items.extend(items)
                    # 避免请求过快，添加延迟
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.warning(f"爬取页面 {url} 失败: {str(e)}，继续尝试下一个URL")
        except Exception as e:
            self.logger.error(f"爬取数据异常: {str(e)}")
        
        return all_items
    
    async def _fetch_page_data(self, url: str) -> List[Dict]:
        """获取单页数据，增强降级策略和错误处理"""
        items = []
        retry_count = 0
        
        while retry_count < MAX_RETRY:
            try:
                # 获取cookies，降级处理获取失败的情况
                try:
                    cookies = await self._get_or_update_cookies()
                except Exception as cookie_error:
                    self.logger.warning(f"获取cookies失败: {str(cookie_error)}，使用空cookies继续")
                    cookies = self.cookies if hasattr(self, 'cookies') else {}
                
                headers = {
                    'User-Agent': random_user_agent(),
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': 'https://www.smzdm.com/',
                }
                
                async with aiohttp.ClientSession(headers=headers) as session:
                    try:
                        async with session.get(url, cookies=cookies, timeout=30) as response:
                            # 检查响应状态码
                            if response.status != 200:
                                self.logger.warning(f"请求失败: HTTP {response.status}")
                                # 403或401等授权错误时尝试更新cookie，但不中断流程
                                if response.status in [401, 403]:
                                    try:
                                        await self._update_cookies()
                                    except Exception:
                                        pass  # 静默处理cookie更新失败
                                # 继续重试，不立即中断
                                retry_count += 1
                                continue
                            
                            # 获取响应内容类型
                            content_type = response.headers.get('Content-Type', '')
                            
                            # 尝试根据内容类型处理
                            if 'application/json' in content_type:
                                # 尝试解析JSON
                                try:
                                    data = await response.json()
                                    if data.get('suc') == 1:
                                        items = self._parse_zdm_items(data.get('data', {}).get('goods_list', []))
                                        self.logger.info(f"成功获取{len(items)}条数据")
                                        return items
                                    else:
                                        self.logger.warning(f"接口返回错误: {data.get('error_msg')}")
                                        # 接口失败时尝试更新cookie，但不强制重试
                                        try:
                                            await self._update_cookies()
                                        except Exception:
                                            pass  # 静默处理cookie更新失败
                                except json.JSONDecodeError as json_error:
                                    self.logger.error(f"JSON解析失败: {str(json_error)}")
                                    # 获取响应文本，尝试从HTML中提取信息
                                    try:
                                        text = await response.text()
                                        self.logger.debug(f"HTML响应长度: {len(text)} 字符")
                                        # 可以在这里添加从HTML提取数据的逻辑
                                    except Exception as text_error:
                                        self.logger.error(f"获取响应文本失败: {str(text_error)}")
                            else:
                                self.logger.warning(f"响应不是JSON格式: {content_type}")
                                # 记录HTML响应长度以便调试
                                try:
                                    text = await response.text()
                                    self.logger.debug(f"HTML响应长度: {len(text)} 字符")
                                except Exception:
                                    pass
                    except Exception as request_error:
                        self.logger.error(f"请求处理异常: {str(request_error)}")
            except Exception as e:
                self.logger.error(f"获取页面数据失败: {str(e)}")
            
            retry_count += 1
            if retry_count < MAX_RETRY:
                wait_time = 2 * retry_count
                self.logger.info(f"重试 {retry_count}/{MAX_RETRY}，等待 {wait_time} 秒")
                await asyncio.sleep(wait_time)  # 指数退避
        
        self.logger.warning(f"达到最大重试次数，返回已获取的{len(items)}条数据")
        return items
    
    def _parse_zdm_items(self, goods_list: List[Dict]) -> List[Dict]:
        """解析商品列表"""
        items = []
        
        for goods in goods_list:
            try:
                # 提取基本信息
                article_id = str(goods.get('article_id', ''))
                title = goods.get('article_title', '').strip()
                url = goods.get('article_url', '')
                
                if not article_id or not title or not url:
                    continue
                
                # 提取价格信息
                price = goods.get('article_price', '')
                
                # 提取图片链接
                pic_url = goods.get('article_pic_url', '')
                
                # 提取商城信息
                article_mall = goods.get('article_mall', '')
                
                # 提取点赞数
                voted = str_number_format(goods.get('article_vote', '0'))
                
                # 提取评论数
                comments = str_number_format(goods.get('article_comment', '0'))
                
                # 提取发布时间
                article_time = goods.get('article_time', '')
                
                # 创建商品对象
                item = {
                    'article_id': article_id,
                    'title': title,
                    'url': url,
                    'price': price,
                    'pic_url': pic_url,
                    'article_mall': article_mall,
                    'voted': voted,
                    'comments': comments,
                    'article_time': article_time,
                    'pushed': False
                }
                
                items.append(item)
            except Exception as e:
                self.logger.error(f"解析商品失败: {str(e)}")
        
        return items
    
    async def _get_or_update_cookies(self) -> Dict[str, str]:
        """获取或更新cookies"""
        current_time = time.time()
        
        # 检查是否需要更新cookie
        if (not self.cookies or 
            current_time - self.cookies_last_updated > self.cookies_expire_time):
            await self._update_cookies()
        
        return self.cookies
    
    async def _update_cookies(self):
        """更新cookies，增强降级策略，修复WebDriver Manager路径问题"""
        try:
            # 检查是否在CI环境中（GitHub Actions等）
            is_ci = os.environ.get('CI', 'false').lower() == 'true' or os.environ.get('GITHUB_ACTIONS') is not None
            if is_ci:
                self.logger.info("检测到CI环境，跳过Selenium，使用空cookies")
                if not hasattr(self, 'cookies') or not self.cookies:
                    self.cookies = {}
                self.cookies_last_updated = time.time()
                return
            
            # 尝试从文件加载
            if os.path.exists(self.cookies_file):
                try:
                    with open(self.cookies_file, 'r', encoding='utf-8') as f:
                        cookie_data = json.load(f)
                        self.cookies = cookie_data.get('cookies', {})
                        self.cookies_last_updated = cookie_data.get('timestamp', 0)
                        
                        # 检查是否过期
                        if time.time() - self.cookies_last_updated < self.cookies_expire_time:
                            self.logger.info("从文件加载有效的cookies")
                            return
                except Exception as e:
                    self.logger.warning(f"读取cookies文件失败: {str(e)}")
            
            # 为了避免WebDriver Manager路径问题，优先使用静态cookies或空cookies
            self.logger.info("跳过Selenium和WebDriver Manager，使用空cookies")
            # 设置一些可能有用的默认cookies
            default_cookies = {
                'device_id': str(int(time.time())),
                'session': f'session_{int(time.time())}',
            }
            
            # 如果已有cookies，保留它们，否则使用默认值
            if not hasattr(self, 'cookies') or not self.cookies:
                self.cookies = default_cookies
            
            self.cookies_last_updated = time.time()
            
            # 保存到文件
            try:
                with open(self.cookies_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'cookies': self.cookies,
                        'timestamp': self.cookies_last_updated
                    }, f, ensure_ascii=False, indent=2)
            except Exception as save_error:
                self.logger.warning(f"保存cookies文件失败: {str(save_error)}")
                
            self.logger.info("使用降级策略设置cookies完成")
            
        except Exception as e:
            self.logger.error(f"更新cookie发生意外错误: {str(e)}")
            # 确保cookies初始化
            if not hasattr(self, 'cookies') or not self.cookies:
                self.cookies = {}  # 确保cookies是一个字典而不是None
            self.cookies_last_updated = time.time()
    
    def clear_cookies(self):
        """清除cookie"""
        self.cookies = None
        self.cookies_last_updated = 0
        if os.path.exists(self.cookies_file):
            try:
                os.remove(self.cookies_file)
            except Exception as e:
                self.logger.error(f"清除cookie文件失败: {str(e)}")