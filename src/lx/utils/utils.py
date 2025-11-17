"""工具方法实现"""
import random
import re
from typing import List
from .constants import USER_AGENTS


def random_user_agent() -> str:
    """随机获取用户代理"""
    return random.choice(USER_AGENTS)


def str_number_format(s: str) -> int:
    """处理k/w单位的数字格式化
    
    Args:
        s: 字符串形式的数字，可能包含k/w等单位
    
    Returns:
        转换后的整数
    """
    if not s or s.strip() == '':
        return 0
    
    s = s.strip()
    # 处理k单位（千）
    if s.endswith('k') or s.endswith('K'):
        try:
            return int(float(s[:-1]) * 1000)
        except ValueError:
            return 0
    # 处理w单位（万）
    elif s.endswith('w') or s.endswith('W'):
        try:
            return int(float(s[:-1]) * 10000)
        except ValueError:
            return 0
    # 尝试直接转换为整数
    else:
        try:
            # 移除非数字字符
            num_str = re.sub(r'[^0-9]', '', s)
            return int(num_str) if num_str else 0
        except ValueError:
            return 0


def build_message(items: List[dict]) -> str:
    """生成HTML表格消息
    
    Args:
        items: 优惠信息列表
    
    Returns:
        HTML格式的消息字符串
    """
    if not items:
        return "暂无优惠信息"
    
    html = """
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;">
            <th style="text-align: left;">标题</th>
            <th style="text-align: left;">价格</th>
            <th style="text-align: left;">商城</th>
            <th style="text-align: left;">点赞</th>
            <th style="text-align: left;">评论</th>
        </tr>
    """
    
    for item in items:
        title = item.get('title', '')
        url = item.get('url', '')
        price = item.get('price', '')
        mall = item.get('article_mall', '')
        voted = item.get('voted', 0)
        comments = item.get('comments', 0)
        
        title_html = f'<a href="{url}" target="_blank">{title}</a>'
        price_html = f'<font color="red">{price}</font>' if price else ''
        
        html += f"""
        <tr>
            <td>{title_html}</td>
            <td>{price_html}</td>
            <td>{mall}</td>
            <td>{voted}</td>
            <td>{comments}</td>
        </tr>
        """
    
    html += "</table>"
    return html


def read_file(file_path: str) -> set:
    """读取文件内容到HashSet
    
    Args:
        file_path: 文件路径
    
    Returns:
        包含文件内容的集合
    """
    result = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    result.add(line)
    except Exception:
        pass
    return result


def write_file(file_path: str, content: str):
    """按行写入文件
    
    Args:
        file_path: 文件路径
        content: 要写入的内容
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    except Exception:
        pass