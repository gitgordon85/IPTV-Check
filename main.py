import asyncio
import copy
import datetime
import gzip
import os
import pickle
import aiohttp  # 新增导入
from time import time

import pytz
from tqdm import tqdm

import utils.constants as constants
from updates.epg import get_epg
from updates.fofa import get_channels_by_fofa
from updates.hotel import get_channels_by_hotel
from updates.multicast import get_channels_by_multicast
from updates.online_search import get_channels_by_online_search
from updates.subscribe import get_channels_by_subscribe_urls
from utils.channel import (
    get_channel_items,
    append_total_data,
    test_speed,
    write_channel_to_file, sort_channel_result,
)
from utils.config import config
from utils.tools import (
    get_pbar_remaining,
    get_ip_address,
    process_nested_dict,
    format_interval,
    check_ipv6_support,
    get_urls_from_file,
    get_version_info,
    join_url,
    get_urls_len,
    merge_objects
)
from utils.types import CategoryChannelData


class UpdateSource:

    def __init__(self):
        self.update_progress = None
        self.run_ui = False
        self.tasks = []
        self.channel_items: CategoryChannelData = {}
        self.hotel_fofa_result = {}
        self.hotel_foodie_result = {}
        self.multicast_result = {}
        self.subscribe_result = {}
        self.online_search_result = {}
        self.epg_result = {}
        self.channel_data: CategoryChannelData = {}
        self.pbar = None
        self.total = 0
        self.start_time = None
        self.stop_event = None
        self.ipv6_support = False
        self.now = None

    async def update_guangdong_sources(self):
        """从指定URL更新广东移动源到本地文件"""
        if not config.open_update:  # 只在开启更新时执行
            return
            
        print("🔄 正在更新广东移动源...")
        url = "https://chuxinya.top/f/AD5QHE/%E6%B5%B7%E7%87%95.txt"
        local_path = config.local_file
        
        try:
            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as response:
                    content = await response.text()
                    
                    # 提取广东移动分组
                    result = []
                    in_target_group = False
                    
                    for line in content.split('\n'):
                        stripped = line.strip()
                        
                        # 检测分组标题行
                        if "广东移动,#genre#" in stripped:
                            in_target_group = True
                            continue
                        elif ",#genre#" in stripped:
                            # 遇到新分组时停止
                            in_target_group = False
                            continue
                            
                        # 在目标分组内收集频道
                        if in_target_group and stripped and "," in stripped:
                            result.append(stripped)
                    
                    if not result:
                        print("⚠️ 未找到广东移动源")
                        return
                        
                    # 读取现有本地源（如果存在）
                    existing_content = []
                    if os.path.exists(local_path):
                        with open(local_path, 'r', encoding='utf-8') as f:
                            existing_content = f.read().splitlines()
                    
                    # 检查是否已有广东移动源标记
                    guangdong_marker = "# === 广东移动源 ==="
                    has_marker = guangdong_marker in existing_content
                    
                    # 如果已有标记且文件不空，删除旧分区
                    if has_marker:
                        # 查找分区开始和结束位置
                        start_idx = existing_content.index(guangdong_marker)
                        end_idx = len(existing_content)
                        
                        # 寻找下一个分区标记或文件结束
                        for i in range(start_idx + 1, len(existing_content)):
                            if existing_content[i].startswith("# ===") or i == len(existing_content) - 1:
                                end_idx = i + 1
                                break
                        
                        # 删除旧分区内容
                        new_content = existing_content[:start_idx] + existing_content[end_idx:]
                    else:
                        new_content = existing_content
                    
                    # 添加分区标记和所有新源
                    if result:
                        # 确保有空白行分隔
                        if new_content and not new_content[-1].strip() == "":
                            new_content.append("")
                        
                        # 添加分区标记
                        new_content.append(guangdong_marker)
                        
                        # 添加所有频道源
                        for line in result:
                            new_content.append(line)
                    
                    # 写入文件
                    with open(local_path, 'w', encoding='utf-8') as f:
                        f.write("\n".join(new_content))
                    
                    print(f"✅ 成功更新广东移动源，添加 {len(result)} 个频道")
        
        except Exception as e:
            print(f"❌ 更新广东移动源失败: {str(e)}")
            # 失败时不中断主流程

    def clean_url(self, url: str) -> str:
        """清理URL后缀（以$开头的部分）"""
        dollar_index = url.find('$')
        if dollar_index != -1:
            return url[:dollar_index]
        return url

    def clean_source_urls(self, data):
        """递归清理数据源中的URL"""
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "url" and isinstance(value, str):
                    data[key] = self.clean_url(value)
                elif isinstance(value, (dict, list)):
                    self.clean_source_urls(value)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self.clean_source_urls(item)

    async def visit_page(self, channel_names: list[str] = None):
        """获取频道数据源"""
        # 更新广东移动源到本地文件（先于其他操作）
        await self.update_guangdong_sources()
        
        tasks_config = [
            ("hotel_fofa", get_channels_by_fofa, "hotel_fofa_result"),
            ("multicast", get_channels_by_multicast, "multicast_result"),
            ("hotel_foodie", get_channels_by_hotel, "hotel_foodie_result"),
            ("subscribe", get_channels_by_subscribe_urls, "subscribe_result"),
            (
                "online_search",
                get_channels_by_online_search,
                "online_search_result",
            ),
            ("epg", get_epg, "epg_result"),
        ]

        for setting, task_func, result_attr in tasks_config:
            if (
                    setting == "hotel_foodie" or setting == "hotel_fofa"
            ) and config.open_hotel == False:
                continue
            if config.open_method[setting]:
                if setting == "subscribe":
                    subscribe_urls = get_urls_from_file(constants.subscribe_path)
                    whitelist_urls = get_urls_from_file(constants.whitelist_path)
                    if not os.getenv("GITHUB_ACTIONS") and config.cdn_url:
                        subscribe_urls = [join_url(config.cdn_url, url) if "raw.githubusercontent.com" in url else url
                                          for url in subscribe_urls]
                    task = asyncio.create_task(
                        task_func(subscribe_urls,
                                  names=channel_names,
                                  whitelist=whitelist_urls,
                                  callback=self.update_progress
                                  )
                    )
                elif setting == "hotel_foodie" or setting == "hotel_fofa":
                    task = asyncio.create_task(task_func(callback=self.update_progress))
                else:
                    task = asyncio.create_task(
                        task_func(channel_names, callback=self.update_progress)
                    )
                self.tasks.append(task)
                setattr(self, result_attr, await task)

    def pbar_update(self, name: str = "", item_name: str = ""):
        if self.pbar.n < self.total:
            self.pbar.update()
            self.update_progress(
                f"正在进行{name}, 剩余{self.total - self.pbar.n}个{item_name}, 预计剩余时间: {get_pbar_remaining(n=self.pbar.n, total=self.total, start_time=self.start_time)}",
                int((self.pbar.n / self.total) * 100),
            )

    async def main(self):
        try:
            main_start_time = time()
            if config.open_update:
                self.channel_items = get_channel_items()
                channel_names = [
                    name
                    for channel_obj in self.channel_items.values()
                    for name in channel_obj.keys()
                ]
                if not channel_names:
                    print(f"❌ No channel names found! Please check the {config.source_file}!")
                    return
                await self.visit_page(channel_names)
                self.tasks = []
                
                sources_to_clean = [
                    self.hotel_fofa_result,
                    self.multicast_result,
                    self.hotel_foodie_result,
                    self.subscribe_result,
                    self.online_search_result
                ]
                
                for source in sources_to_clean:
                    self.clean_source_urls(source)
                
                append_total_data(
                    self.channel_items.items(),
                    self.channel_data,
                    self.hotel_fofa_result,
                    self.multicast_result,
                    self.hotel_foodie_result,
                    self.subscribe_result,
                    self.online_search_result,
                )
                cache_result = self.channel_data
                test_result = {}
                if config.open_speed_test:
                    urls_total = get_urls_len(self.channel_data)
                    test_data = copy.deepcopy(self.channel_data)
                    process_nested_dict(
                        test_data,
                        seen=set(),
                        filter_host=config.speed_test_filter_host,
                        ipv6_support=self.ipv6_support
                    )
                    self.total = get_urls_len(test_data)
                    self.update_progress(
                        f"正在进行测速, 共{urls_total}个接口, {self.total}个接口需要进行测速",
                        0,
                    )
                    self.start_time = time()
                    self.pbar = tqdm(total=self.total, desc="Speed test")
                    test_result = await test_speed(
                        test_data,
                        ipv6=self.ipv6_support,
                        callback=lambda: self.pbar_update(name="测速", item_name="接口"),
                    )
                    cache_result = merge_objects(cache_result, test_result, match_key="url")
                    self.pbar.close()
                self.channel_data = sort_channel_result(
                    self.channel_data,
                    result=test_result,
                    filter_host=config.speed_test_filter_host,
                    ipv6_support=self.ipv6_support
                )
                self.update_progress(f"正在生成结果文件", 0)
                write_channel_to_file(
                    self.channel_data,
                    epg=self.epg_result,
                    ipv6=self.ipv6_support,
                    first_channel_name=channel_names[0],
                )
                if config.open_history:
                    if os.path.exists(constants.cache_path):
                        with gzip.open(constants.cache_path, "rb") as file:
                            try:
                                cache = pickle.load(file)
                            except EOFError:
                                cache = {}
                            cache_result = merge_objects(cache, cache_result, match_key="url")
                    with gzip.open(constants.cache_path, "wb") as file:
                        pickle.dump(cache_result, file)
                print(
                    f"🥳 Update completed! Total time spent: {format_interval(time() - main_start_time)}."
                )
            if self.run_ui:
                open_service = config.open_service
                service_tip = ", 可使用以下地址进行观看" if open_service else ""
                tip = (
                    f"✅ 服务启动成功{service_tip}"
                    if open_service and config.open_update == False
                    else f"🥳更新完成, 耗时: {format_interval(time() - main_start_time)}{service_tip}"
                )
                self.update_progress(
                    tip,
                    100,
                    finished=True,
                    url=f"{get_ip_address()}" if open_service else None,
                    now=self.now
                )
        except asyncio.exceptions.CancelledError:
            print("Update cancelled!")

    async def start(self, callback=None):
        def default_callback(self, *args, **kwargs):
            pass

        self.update_progress = callback or default_callback
        self.run_ui = True if callback else False
        if self.run_ui:
            self.update_progress(f"正在检查网络是否支持IPv6", 0)
        self.ipv6_support = config.ipv6_support or check_ipv6_support()
        if not os.getenv("GITHUB_ACTIONS") and config.update_interval:
            await self.scheduler(asyncio.Event())
        else:
            await self.main()

    def stop(self):
        for task in self.tasks:
            task.cancel()
        self.tasks = []
        if self.pbar:
            self.pbar.close()
        if self.stop_event:
            self.stop_event.set()

    async def scheduler(self, stop_event):
        self.stop_event = stop_event
        while not stop_event.is_set():
            self.now = datetime.datetime.now(pytz.timezone(config.time_zone))
            await self.main()
            next_time = self.now + datetime.timedelta(hours=config.update_interval)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=config.update_interval * 3600)
            except asyncio.TimeoutError:
                continue


if __name__ == "__main__":
    info = get_version_info()
    print(f"✡️ {info['name']} Version: {info['version']}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    update_source = UpdateSource()
    loop.run_until_complete(update_source.start())
