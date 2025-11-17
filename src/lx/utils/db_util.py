import aiosqlite
import asyncio
from typing import List, Optional, Set
from datetime import datetime, timedelta


class DatabaseUtil:
    """数据库工具类，处理SQLite数据库操作"""
    
    def __init__(self, db_path: str = 'database.db'):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """连接数据库"""
        if not self._conn:
            self._conn = await aiosqlite.connect(self.db_path)
            await self._create_tables()
        return self._conn
    
    async def disconnect(self):
        """断开数据库连接"""
        if self._conn:
            await self._conn.close()
            self._conn = None
    
    async def close(self):
        """关闭数据库连接（兼容旧版本）"""
        await self.disconnect()
    
    async def _create_tables(self):
        """创建必要的数据库表"""
        conn = await self.connect()
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ZDM (
                article_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                article_pic_url TEXT,
                price TEXT,
                voted INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                article_mall TEXT,
                article_time TEXT,
                pushed INTEGER DEFAULT 0
            )
        ''')
        await conn.commit()
    
    async def save_or_update(self, data: dict):
        """保存或更新单条记录"""
        conn = await self.connect()
        await conn.execute('''
            INSERT OR REPLACE INTO ZDM 
            (article_id, title, url, article_pic_url, price, voted, comments, article_mall, article_time, pushed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('article_id'),
            data.get('title'),
            data.get('url'),
            data.get('pic_url'),
            data.get('price'),
            data.get('voted', 0),
            data.get('comments', 0),
            data.get('article_mall'),
            data.get('article_time'),
            1 if data.get('pushed', False) else 0
        ))
        await conn.commit()
    
    async def save_or_update_batch(self, data_list: List[dict]):
        """批量保存或更新记录"""
        conn = await self.connect()
        async with conn.execute_batch('''
            INSERT OR REPLACE INTO ZDM 
            (article_id, title, url, article_pic_url, price, voted, comments, article_mall, article_time, pushed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [(
            item.get('article_id'),
            item.get('title'),
            item.get('url'),
            item.get('pic_url'),
            item.get('price'),
            item.get('voted', 0),
            item.get('comments', 0),
            item.get('article_mall'),
            item.get('article_time'),
            1 if item.get('pushed', False) else 0
        ) for item in data_list]):
            await conn.commit()
    
    async def get_unpushed_records(self) -> List[dict]:
        """获取未推送的记录"""
        conn = await self.connect()
        async with conn.execute('SELECT * FROM ZDM WHERE pushed = 0') as cursor:
            columns = [desc[0] for desc in cursor.description]
            result = []
            async for row in cursor:
                row_dict = dict(zip(columns, row))
                # 转换字段名和数据类型
                row_dict['pic_url'] = row_dict.pop('article_pic_url')
                row_dict['pushed'] = bool(row_dict['pushed'])
                result.append(row_dict)
            return result
    
    async def get_pushed_ids(self) -> Set[str]:
        """获取最近一个月内已推送的article_id集合"""
        conn = await self.connect()
        # 计算一个月前的时间
        one_month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        async with conn.execute(
            'SELECT article_id FROM ZDM WHERE pushed = 1 AND article_time >= ?',
            (one_month_ago,)
        ) as cursor:
            result = set()
            async for row in cursor:
                result.add(row[0])
            return result
    
    async def update_pushed_status(self, article_ids: List[str], pushed: bool = True):
        """更新推送状态"""
        conn = await self.connect()
        pushed_int = 1 if pushed else 0
        await conn.execute(
            f'UPDATE ZDM SET pushed = {pushed_int} WHERE article_id IN ({"?,".join(["?"] * len(article_ids))[:-1]})',
            article_ids
        )
        await conn.commit()