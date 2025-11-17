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
        """创建必要的数据库表，确保字段名称正确"""
        conn = await self.connect()
        # 检查表是否存在
        async with conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ZDM'") as cursor:
            table_exists = await cursor.fetchone() is not None
        
        if table_exists:
            # 检查表结构，确保有所有必要的列
            async with conn.execute("PRAGMA table_info(ZDM)") as cursor:
                columns = {row[1] for row in await cursor.fetchall()}
            
            # 如果缺少必要的列，重建表
            required_columns = {'article_id', 'title', 'url', 'article_pic_url', 'price', 'voted', 'comments', 'article_mall', 'article_time', 'pushed'}
            if not required_columns.issubset(columns):
                # 备份原表数据
                await conn.execute("ALTER TABLE ZDM RENAME TO ZDM_old")
                
                # 创建新表
                await conn.execute('''
                    CREATE TABLE ZDM (
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
                
                # 尝试恢复可能的数据
                try:
                    await conn.execute("INSERT INTO ZDM SELECT * FROM ZDM_old")
                except:
                    # 如果恢复失败，忽略错误
                    pass
                
                # 删除旧表
                await conn.execute("DROP TABLE IF EXISTS ZDM_old")
        else:
            # 创建新表
            await conn.execute('''
                CREATE TABLE ZDM (
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
        """批量保存或更新记录，使用异步方式插入"""
        conn = await self.connect()
        try:
            # 使用参数化查询，逐条执行
            sql = '''
                INSERT OR REPLACE INTO ZDM 
                (article_id, title, url, article_pic_url, price, voted, comments, article_mall, article_time, pushed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # 逐条插入（aiosqlite不支持execute_batch的async with语法）
            for item in data_list:
                await conn.execute(sql, (
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
                ))
            
            await conn.commit()
        except Exception as e:
            # 发生错误时回滚
            await conn.rollback()
            raise e
    
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