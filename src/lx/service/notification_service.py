"""推送服务类，负责邮件和微信推送"""
import asyncio
import aiohttp
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional

from src.lx.utils.constants import WXPUSHER_URL
from src.lx.utils.utils import build_message


class NotificationService:
    """通知推送服务"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 邮件配置
        self.email_config = None
        # 微信推送配置
        self.wx_config = None
    
    def set_email_config(self, host: str, port: int, username: str, password: str, from_addr: str, to_addrs: List[str]):
        """设置邮件配置
        
        Args:
            host: SMTP服务器地址
            port: SMTP服务器端口
            username: 用户名
            password: 密码
            from_addr: 发件人地址
            to_addrs: 收件人地址列表
        """
        self.email_config = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'from_addr': from_addr,
            'to_addrs': to_addrs
        }
        self.logger.info(f"邮件配置已设置: 从 {from_addr} 发送到 {len(to_addrs)} 个收件人")
    
    def set_wx_config(self, app_token: str, content_type: int = 3, topic_ids: List[int] = None, uids: List[str] = None):
        """设置微信推送配置
        
        Args:
            app_token: WxPusher的AppToken
            content_type: 内容类型，3为HTML，1为普通文本
            topic_ids: 主题ID列表
            uids: 用户ID列表
        """
        self.wx_config = {
            'app_token': app_token,
            'content_type': content_type,
            'topic_ids': topic_ids or [],
            'uids': uids or []
        }
        self.logger.info(f"微信推送配置已设置: AppToken={app_token}")
    
    async def push_to_email(self, items: List[Dict]) -> bool:
        """推送商品信息到邮件
        
        Args:
            items: 商品列表
        
        Returns:
            是否推送成功
        """
        if not self.email_config:
            self.logger.error("邮件配置未设置")
            return False
        
        if not items:
            self.logger.info("没有商品信息需要推送")
            return True
        
        try:
            # 构建HTML消息
            html_content = build_message(items)
            
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from_addr']
            msg['To'] = ', '.join(self.email_config['to_addrs'])
            msg['Subject'] = f"【什么值得买】发现 {len(items)} 个优质优惠商品"
            
            # 添加HTML内容
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            # 发送邮件
            with smtplib.SMTP(self.email_config['host'], self.email_config['port']) as server:
                server.starttls()
                server.login(self.email_config['username'], self.email_config['password'])
                server.send_message(msg)
            
            self.logger.info(f"邮件推送成功，共推送 {len(items)} 个商品")
            return True
        except Exception as e:
            self.logger.error(f"邮件推送失败: {str(e)}")
            return False
    
    async def push_to_wechat(self, items: List[Dict]) -> bool:
        """推送商品信息到微信
        
        Args:
            items: 商品列表
        
        Returns:
            是否推送成功
        """
        if not self.wx_config:
            self.logger.error("微信推送配置未设置")
            return False
        
        if not items:
            self.logger.info("没有商品信息需要推送")
            return True
        
        try:
            # 构建HTML消息
            html_content = build_message(items)
            
            # 构建推送参数
            payload = {
                'appToken': self.wx_config['app_token'],
                'content': html_content,
                'contentType': self.wx_config['content_type'],
                'topicIds': self.wx_config['topic_ids'],
                'uids': self.wx_config['uids'],
                'url': 'https://www.smzdm.com/',
                'title': f"什么值得买 - {len(items)} 个优质优惠商品"
            }
            
            # 发送请求
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    WXPUSHER_URL,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('code') == 1000:
                            self.logger.info(f"微信推送成功，共推送 {len(items)} 个商品")
                            return True
                        else:
                            self.logger.error(f"微信推送失败: {result.get('msg')}")
                    else:
                        self.logger.error(f"微信推送请求失败: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"微信推送异常: {str(e)}")
        
        return False
    
    async def push_notifications(self, items: List[Dict], use_email: bool = True, use_wechat: bool = True) -> Dict[str, bool]:
        """推送通知到多个渠道
        
        Args:
            items: 商品列表
            use_email: 是否使用邮件推送
            use_wechat: 是否使用微信推送
        
        Returns:
            各渠道推送结果
        """
        results = {}
        
        tasks = []
        if use_email:
            tasks.append(("email", self.push_to_email(items)))
        if use_wechat:
            tasks.append(("wechat", self.push_to_wechat(items)))
        
        # 并行执行推送任务
        for channel, task in tasks:
            results[channel] = await task
        
        return results