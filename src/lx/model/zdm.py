from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional


class Zdm(BaseModel):
    """什么值得买优惠信息模型"""
    model_config = ConfigDict(from_attributes=True)
    
    article_id: str  # 文章ID，作为主键
    title: str  # 标题
    url: str  # 链接
    pic_url: Optional[str] = None  # 图片链接
    price: Optional[str] = None  # 价格
    voted: Optional[int] = 0  # 点赞数
    comments: Optional[int] = 0  # 评论数
    article_mall: Optional[str] = None  # 商城
    article_time: Optional[str] = None  # 发布时间
    pushed: bool = False  # 是否已推送
    timesort: Optional[float] = None  # 非持久化字段，用于排序
    
    def to_html_tr(self) -> str:
        """将优惠信息转换为HTML表格行"""
        title_html = f'<a href="{self.url}" target="_blank">{self.title}</a>'
        price_html = f'<font color="red">{self.price}</font>' if self.price else ''
        mall_html = self.article_mall or ''
        
        return f"""
        <tr>
            <td>{title_html}</td>
            <td>{price_html}</td>
            <td>{mall_html}</td>
            <td>{self.voted}</td>
            <td>{self.comments}</td>
        </tr>
        """
    
    def __eq__(self, other):
        if not isinstance(other, Zdm):
            return False
        return self.article_id == other.article_id
    
    def __hash__(self):
        return hash(self.article_id)