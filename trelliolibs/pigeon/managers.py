import time
import copy
import asyncio
import uuid
from .channels import *

def uuid_serializer(data):
    for key, value in data.items():
        if isinstance(value, uuid.UUID):
            data[key] = str(value)
        if value is None:
            data[key] = ''
    return data

def current_millitime():
    return int(round(time.time() * 1000))

class Notification:
    '''
    sample msg dict
    {
	"channels": ["email"],
	"receivers": {"email":["ema@test.com"]},
	"content": "this is content",
	"template": {},
	"channel_ds": {"email":{"subject":"lorem ipsum"}}
    }
    '''

    def notify(self, message_dict):
        status_iter = []
        channels = self.get_channels(message_dict)
        for ch in channels:
            cpy_msg_dict = copy.deepcopy(message_dict)
            channel_dict = ch.extract(cpy_msg_dict)
            rt = ch.send(channel_dict,loop=asyncio.get_event_loop(), done_callback=self.notification_sent_callback,
                         done_kwargs={})
            status_iter.append(rt)
        return status_iter

    def get_channels(self, message_dict):
        channels_list = []
        for i in message_dict['channels']:
            channels_list.append(CHANNEL_MATRIX[i]())#creating channel object
        return channels_list

    def notification_sent_callback(self, _future, _loop, *args, **kwargs):#will be called after every notification sent
        result = _future.result()#to raise exception if failed


# class DatabaseNotification(Notification):
#
#
#     def __init__(self):
#         self.table_name = 'notification_dump'
#
#
#     @async_atomic()
#     async def get_unsent_notifications_dict(self, *args, **kwargs):
#         conn = kwargs['conn']
#         query_str = '''
#         SELECT * FROM {table_name} WHERE sent=false;
#         '''
#         res = await conn.fetch(query_str.format_map(**{'table_name': self.table_name}))
#         return RecordHelper.record_to_dict(res)
#
#     @async_atomic(raise_exception=True)
#     async def db_notify(self, message_dict, *args, **kwargs):
#         conn = kwargs['conn']
#         status_iter = []
#         channels = self.get_channels(message_dict)
#         notification_dict = await self.create_notification(message_dict, conn=conn)
#         for ch in channels:
#             cpy_msg_dict = copy.deepcopy(message_dict)
#             channel_dict = ch.extract(cpy_msg_dict)
#             done_callback = self.notification_sent_callback
#             done_kwargs = {'notification_id': notification_dict['id']}
#             rt = ch.send(channel_dict, loop=asyncio.get_event_loop(), done_callback=done_callback, done_kwargs=done_kwargs)
#             status_iter.append(rt)
#         return status_iter
#
#
#     @async_atomic()
#     async def update_notification(self, update_dict, *args, **kwargs):
#         conn = kwargs['conn']
#         query_str = '''
#         UPDATE {table_name} SET {set_str} WHERE id='{id}' RETURNING *;
#
#         '''
#         set_str = ''
#         if update_dict.get('sent'):
#             set_str += "{col}='{val}'".format(col='sent', val='true' if update_dict.get('sent') else 'false')
#         if update_dict.get('sent_at'):
#             if set_str:
#                 set_str += ','
#             set_str += "{col}={val}".format(col='sent_at', val=str(update_dict.get('sent_at')))
#         if update_dict.get('send_at'):
#             if set_str:
#                 set_str +=','
#             set_str += "{col}={val}'".format(col='send_at', val=str(update_dict.get('send_at')))
#         if update_dict.get('notification_dict'):
#             if set_str:
#                 set_str += ','
#             set_str += "{col}='{val}'".format(col='notification_dict', val=str(update_dict.get('notification_dict')))
#         query_dict = {'id': update_dict['id'], 'set_str': set_str, 'table_name': self.table_name}
#         result = await conn.fetch(query_str.format(**query_dict))
#         return result
#
#     @async_atomic()
#     async def create_notification(self, message_dict, *args, **kwargs):#DUMP TO DATABASE
#         conn = kwargs['conn']
#         query_str = '''
#         INSERT INTO {table_name} (notification_dict) VALUES ('{notification_dict}') RETURNING *;
#         '''
#         query_dict = {'table_name': self.table_name, 'notification_dict': json.dumps(message_dict)}
#         result = await conn.fetch(query_str.format(**query_dict))
#         return RecordHelper.record_to_dict(result, normalize=uuid_serializer)
#
#     def notification_sent_callback(self, _future, _loop, *args, **kwargs):#will be called after every notification sent
#         notification_id = kwargs.get('notification_id')
#         super(DatabaseNotification,self).notification_sent_callback(_future,_loop)
#         update_dict = {'sent': True, 'sent_at': int(round(time.time() * 1000)), 'id': notification_id}
#         x= asyncio.run_coroutine_threadsafe(self.update_notification(update_dict), _loop)
#         return x