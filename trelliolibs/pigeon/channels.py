import json
import logging
from .workers import get_notification_worker
from .templates import *

class InvalidMessageError(Exception):
    pass

class BaseChannel:
    _base_json = {'channels': [],
                  'receivers': {},#channel_name: recv_list
                  'content': '',
                  'template': {},#channel_name: template_id
                  'channel_ds': {}#channel specific information e.g attachments for email,
                  # title for android pushes etc
                  }
    template_dict = None
    _CHANNEL_NAME = 'BaseChannel'


    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def blocking_send(self, *args, **kwargs):
        raise NotImplementedError

    def send(self, msg_dict, done_callback=lambda:None, done_kwargs={}, loop=None):
        template = msg_dict['template']
        final_dict = template.render(msg_dict)
        no_lo = get_notification_worker()
        res =  no_lo.submit(self.blocking_send, method_kwargs={'msg_dict': final_dict}, done_callback=done_callback,
                     done_kwargs=done_kwargs, loop=loop)
        return res

    def extract(self, full_msg_dict):
        msg_dict = {}
        channels = full_msg_dict['channels']
        if not (self._CHANNEL_NAME in channels):
            raise InvalidMessageError
        recvs = full_msg_dict['receivers'][self._CHANNEL_NAME]
        msg_dict['content'] = full_msg_dict['content']
        msg_dict['receivers'] = recvs
        msg_dict['template'] = self.get_template(full_msg_dict)
        msg_dict.update(full_msg_dict['channel_ds'][self._CHANNEL_NAME])
        return msg_dict

    def get_template(self, msg_dict=None):
        try:
            return self.template_dict[msg_dict['template'][self._CHANNEL_NAME]]
        except:
            return self.template_dict['default']

class EmailChannel(BaseChannel):
    template_dict = {'default': DefaultEmailTemplate()}

    _CHANNEL_NAME = 'email'

    def blocking_send(self, msg_dict, *args, **kwargs):
        from .email import send
        del msg_dict['template']
        self._logger.info('sending email for %s' %(json.dumps(msg_dict)))
        res = send(msg_dict['receivers'], msg_dict['subject'], msg_dict['content'])
        self._logger.info(res)


class AndroidChannel(BaseChannel):
    _CHANNEL_NAME = 'android'

class SMSChannel(BaseChannel):
    _CHANNEL_NAME = 'sms'


CHANNEL_MATRIX = {AndroidChannel._CHANNEL_NAME: AndroidChannel,
                  SMSChannel._CHANNEL_NAME: SMSChannel,
                  EmailChannel._CHANNEL_NAME: EmailChannel}











