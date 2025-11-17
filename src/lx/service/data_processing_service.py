"""数据处理服务类，负责数据过滤和处理"""
import logging
import re
from typing import List, Dict, Set


class DataProcessingService:
    """数据处理服务"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 过滤配置
        self.min_vote_threshold = 10  # 最小点赞数阈值
        self.min_comment_threshold = 5  # 最小评论数阈值
        # 抢购关键词正则
        self.flash_sale_pattern = re.compile(r'前\d+名', re.IGNORECASE)
    
    async def process_data(self, raw_items: List[Dict], pushed_ids: Set[str]) -> List[Dict]:
        """处理原始数据，包括过滤和排序
        
        Args:
            raw_items: 原始抓取的商品列表
            pushed_ids: 已推送的商品ID集合
        
        Returns:
            处理后的商品列表
        """
        # 去重
        unique_items = self._remove_duplicates(raw_items)
        self.logger.info(f"去重后商品数量: {len(unique_items)} -> {len(raw_items)}")
        
        # 过滤
        filtered_items = self._filter_items(unique_items, pushed_ids)
        self.logger.info(f"过滤后商品数量: {len(filtered_items)} / {len(unique_items)}")
        
        # 排序
        sorted_items = self._sort_items(filtered_items)
        
        return sorted_items
    
    def _remove_duplicates(self, items: List[Dict]) -> List[Dict]:
        """去除重复商品，基于article_id"""
        seen_ids = set()
        unique_items = []
        
        for item in items:
            article_id = item.get('article_id')
            if article_id and article_id not in seen_ids:
                seen_ids.add(article_id)
                unique_items.append(item)
        
        return unique_items
    
    def _filter_items(self, items: List[Dict], pushed_ids: Set[str]) -> List[Dict]:
        """过滤商品
        1. 过滤已推送的商品
        2. 过滤点赞数和评论数过低的商品
        3. 过滤抢购类商品
        """
        filtered = []
        
        for item in items:
            # 过滤已推送的商品
            article_id = item.get('article_id')
            if article_id in pushed_ids:
                continue
            
            # 过滤点赞数过低的商品
            voted = item.get('voted', 0)
            if voted < self.min_vote_threshold:
                continue
            
            # 过滤评论数过低的商品
            comments = item.get('comments', 0)
            if comments < self.min_comment_threshold:
                continue
            
            # 过滤抢购类商品
            title = item.get('title', '')
            if self.flash_sale_pattern.search(title):
                continue
            
            # 所有过滤条件都通过
            filtered.append(item)
        
        return filtered
    
    def _sort_items(self, items: List[Dict]) -> List[Dict]:
        """排序商品
        优先按照点赞数降序，然后按照评论数降序
        """
        return sorted(
            items, 
            key=lambda x: (x.get('voted', 0), x.get('comments', 0)), 
            reverse=True
        )
    
    def set_thresholds(self, min_vote: int = None, min_comment: int = None):
        """设置过滤阈值
        
        Args:
            min_vote: 最小点赞数阈值
            min_comment: 最小评论数阈值
        """
        if min_vote is not None:
            self.min_vote_threshold = min_vote
        if min_comment is not None:
            self.min_comment_threshold = min_comment
        
        self.logger.info(f"更新过滤阈值: 点赞数={self.min_vote_threshold}, 评论数={self.min_comment_threshold}")