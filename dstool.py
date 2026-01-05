# encoding: utf-8
# @File: dstool.py
# @Author: neuf
# @Desc: 
# @Date: 2026/1/5

import json
import os
import sys
import tempfile
import time
from urllib.parse import quote_plus

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



class Dstool:
    def __init__(self):
        # self.dsBaseUrl = 'https://bigdata.xxxx.com/dolphinscheduler'
        self.dsBaseUrl = 'http://dolphinscheduler-api:12345/dolphinscheduler'
        self.headers = {
            'accept': '*/*',
            'token': 'dolphinscheduler令牌'
        }
        # 数据库连接配置
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'password',
            'database': 'dolphinscheduler'
        }
        user = quote_plus(self.db_config['user'])
        password = quote_plus(self.db_config['password'])
        charset = self.db_config.get('charset', 'utf8mb4')
        self.db_url = (
            f"mysql+pymysql://{user}:{password}@{self.db_config['host']}:{self.db_config['port']}/"
            f"{self.db_config['database']}?charset={charset}"
        )
        self.engine = create_engine(self.db_url, pool_pre_ping=True, pool_recycle=3600)

    def read_data(self, file_path):
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    fixed = (content
                             .replace('\n', ' ')
                             .replace('\t', ' ')
                             .replace('\r', ' '))
                    return json.loads(fixed)
            return {}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(e)
            return {}

    def extract_ds_params(self, params_str):
        """
        提取dolphinscheduler注入的变量为字典
        :param params_str:
        :return:
        """
        params_dict = {}
        # 按空格分割
        parts = params_str.strip().split()

        for part in parts:
            if part.startswith('-D') and '=' in part:
                # 去掉 -D 前缀
                kv = part[2:]
                key, value = kv.split('=', 1)
                # 去掉值的引号
                value = value.strip('\'"')
                params_dict[key] = value

        return params_dict

    def query_execution_time(self, project_code, task_code):
        """
        从数据库查询任务执行时间，只查询正在执行和执行成功的记录
        :param task_code: 任务代码
        :return: 包含current_exec_time和last_exec_time的字典
        """
        sql_stmt = text(
            """
            SELECT start_time, state
            FROM t_ds_task_instance
            WHERE project_code = :project_code
                AND task_code = :task_code
                AND state IN (1, 7)
            ORDER BY start_time DESC
            LIMIT 2
            """
        )

        try:
            with self.engine.connect() as conn:
                result = conn.execute(sql_stmt, {'project_code': project_code, 'task_code': task_code}).mappings().all()
        except SQLAlchemyError as exc:
            print(f"Database error: {exc}")
            return {}

        if not result:
            print(f"No execution records found for task_code: {task_code}")
            return {}

        time_dict = {'current_exec_time': str(result[0]['start_time'])}

        if len(result) == 2 and result[1]['state'] == 1:
            # 上个任务正在运行，结束当前任务
            print("last execution is running")
            sys.exit(1)
        elif len(result) == 2 and result[1]['state'] == 7:
            time_dict['last_exec_time'] = str(result[1]['start_time'])
            return time_dict
        else:
            # 没有历史数据返回当天00:00:00，或者自行设定返回1900-01-01 00:00:00进行全量抽取
            time_dict['last_exec_time'] = str(result[0]['start_time']).split()[0] + " 00:00:00"
            return time_dict

    def get_execution_time(self, project_code, task_code):
        """
        获取dolphinscheduler api获取任务执行时间，如果前面都是非成功状态的会难以获取上次成功时间，
        这里只查询3条记录，查太多性能差，最后还是走数据库
        :param task_code: 任务代码
        :return: 获取任务执行时间，只查询正在执行和执行成功的记录
        """
        time_dict = {}
        time.sleep(2)
        try:
            task_instances = (requests.get(
                f"{self.dsBaseUrl}/projects/{project_code}/task-instances",
                headers=self.headers,
                params={'taskCode': task_code, 'pageNo': 1, 'pageSize': 3})
                          .json())
        except Exception as e:
            raise f"Error getting from api! {e}"
        totalList = task_instances['data']['totalList']

        time_dict['current_exec_time'] = totalList[0]['startTime']
        if len(totalList) == 1:
            time_dict['last_exec_time'] = (totalList[0]['startTime']).split()[0] + " 00:00:00"
            return time_dict
        elif len(totalList) >= 2 and totalList[1]['state'] == 'RUNNING_EXECUTION':
            print("last execution is running")
            sys.exit(1)
        elif len(totalList) >= 2 and totalList[1]['state'] == 'SUCCESS':
            time_dict['last_exec_time'] = totalList[1]['startTime']
            return time_dict
        elif len(totalList) == 3 and totalList[2]['state'] == 'SUCCESS':
            time_dict['last_exec_time'] = totalList[2]['startTime']
            return time_dict
        else:
            return self.query_execution_time(project_code, task_code)


    def check_params(self, job_config, params):
        """
        读取json检查任务参数，如果参数中包含${last_exec_time}，则查询任务执行时间
        :param job_config:
        :param params:
        :return:
        """
        parameter = job_config['job']['content'][0]['reader']['parameter']

        needs_time = bool('${last_exec_time}' in parameter.get('where', '') or
                           ('connection' in parameter and
                            len(parameter['connection']) > 0 and
                            '${last_exec_time}' in parameter['connection'][0].get('querySql', '')))

        if needs_time:
            ds_params = self.extract_ds_params(params)
            project_code = ds_params.get('system.project.code')
            task_code = ds_params.get('system.task.definition.code')
            exec_time = self.get_execution_time(project_code, task_code)
            # exec_time = self.query_execution_time(project_code, task_code)
            print(exec_time)
            return f" -Dcurrent_exec_time='{exec_time.get('current_exec_time')}' -Dlast_exec_time='{exec_time.get('last_exec_time')}'"
        return ''
