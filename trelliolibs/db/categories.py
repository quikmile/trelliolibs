import sys
import copy
import logging
import uuid
from trelliopg.sql import async_atomic
from trelliolibs.utils.helpers import RecordHelper, uuid_serializer
from collections import deque


class InvalidNodeDict(Exception):
    pass


class RowNotCreated(Exception):
    pass


class RowNotFound(Exception):
    pass


class NodeParentMissing(Exception):
    pass


class InvalidTypeDict(Exception):
    pass


class InvalidArgument(Exception):
    pass


class Stack:
    def __init__(self):
        self._dq = deque()

    def push(self, node):
        self._dq.append(node)

    def pop(self):
        return self._dq.pop()

    def is_empty(self):
        try:
            n = self._dq.pop()
            self._dq.append(n)
            return False
        except IndexError:
            return True

logger = logging.getLogger(__name__)

class NestedCategoriesManager:
    def __init__(self, table_name, left_col_name, right_col_name, group_name, parent_name):
        self.table_name = table_name
        self.left_name = left_col_name
        self.right_name = right_col_name
        self.group_name = group_name
        self.parent_name = parent_name

    @async_atomic(raise_exception=True)
    async def filter_by_params(self, params, group_id='', **kwargs):
        conn = kwargs['conn']
        if group_id:
            params[self.group_name] = group_id
        where_str = 'where '+' and '.join(["{col}='{val}'".format(col=col, val=val) for col, val in params.items()])
        q_s = 'SELECT * FROM {table} {where};'.format(where=where_str,table=self.table_name)
        logger.info(q_s)
        result = await conn.fetch(q_s)
        return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])

    @async_atomic(raise_exception=True)
    async def get_leaves(self, group_id='', **kwargs):
        conn = kwargs['conn']
        query_str = '''SELECT * FROM {table_name}
 					   WHERE {pk} NOT IN (SELECT {parent_name} FROM {table_name} WHERE {parent_name} IS NOT null {only_group}) {only_group};'''
        query_dict = {'table_name': self.table_name, 'pk': 'id', 'parent_name':self.parent_name}
        if group_id:
            query_dict['only_group'] = " and %s='%s' " % (self.group_name, group_id)
        else:
            query_dict['only_group'] = ''
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            return []

    @async_atomic(raise_exception=True)
    async def get_all(self, group_id='', limit=sys.maxsize, **kwargs):
        conn = kwargs['conn']
        query_str = '''SELECT * FROM {table_name} {group_only} ORDER BY {left_col} LIMIT {limit};'''
        if group_id:
            group_only = " WHERE %s='%s' " % (self.group_name, group_id)
        else:
            group_only = ''
        formatted_qs = query_str.format(table_name=self.table_name, limit=limit,
                                                    left_col=self.left_name, group_only=group_only)
        logger.info(formatted_qs)
        records = await conn.fetch(formatted_qs)
        if records:
            return RecordHelper.record_to_dict(records, normalize=[uuid_serializer])
        else:
            return []

    @async_atomic(raise_exception=True)
    async def has_children(self, node_id, **kwargs):  # node_id unique group_id not needed
        conn = kwargs['conn']
        query_str = '''
		SELECT * FROM {table_name} WHERE {parent_name}='{parent_id}' LIMIT 1;
		'''
        logger.info(query_str.format(table_name=self.table_name, parent_name=self.parent_name, parent_id=str(node_id)))
        result = await conn.fetch(
            query_str.format(table_name=self.table_name, parent_name=self.parent_name, parent_id=str(node_id)))
        if result:
            return True
        return False

    @async_atomic(raise_exception=True)
    async def get_children_by_id(self, parent_id, **kwargs):  # parent_id unique group_id not needed
        conn = kwargs['conn']
        query_str = '''
	    				SELECT * FROM {table_name} WHERE {parent_name}='{parent_value}'
	    				ORDER BY {left_col};
	    				'''
        query_dict = {'left_col': self.left_name, 'parent_value': str(parent_id),
                      'table_name': self.table_name, 'parent_name': self.parent_name
                      }
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            return []

    @async_atomic(raise_exception=True)
    async def get_children(self, parent_id, all=True, group_id='', **kwargs):
        conn = kwargs['conn']
        query_str = '''
				SELECT * FROM {table_name} WHERE {left_col}>{left_val} and {right_col}<{right_val} {group_query} {extra}
				ORDER BY {left_col};
				'''
        query_dict = {'left_col': self.left_name, 'right_col': self.right_name,
                      'table_name': self.table_name
                      }
        parent_node = await self.get_node(parent_id, conn=conn)
        left_val = parent_node[self.left_name]
        right_val = parent_node[self.right_name]
        query_dict['left_val'] = left_val
        query_dict['right_val'] = right_val
        if not all:
            query_dict['extra'] = " and %s='%s' " % (self.parent_name, str(parent_id))
        else:
            query_dict['extra'] = ''
        if group_id:
            query_dict['group_query'] = " and %s='%s' " % (self.group_name, group_id)
        else:
            query_dict['group_query'] = ''
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            return []

    @async_atomic(raise_exception=True)
    async def get_node(self, id, **kwargs):  # id is unique group_id is not needed
        conn = kwargs['conn']
        query_str = '''
			SELECT * FROM {table_name} WHERE {where_str};
		'''
        query_dict = {'table_name': self.table_name, 'where_str': ''}
        query_dict['where_str'] = "id='%s'" % (str(id))
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if not result:
            raise RowNotFound
        return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])

    @async_atomic(raise_exception=True)
    async def get_forward_siblings(self, node_dict, group_id='', **kwargs):
        conn = kwargs['conn']
        query_str = '''
		SELECT * FROM {table_name} WHERE {parent_name}='{parent_value}' and {left_col}>{right_val} and id!='{id}'
		ORDER BY {left_col};
		'''
        query_dict = {'table_name': self.table_name, 'parent_value': node_dict[self.parent_name], 'left_col': self.left_name,
                      'right_val': node_dict[self.right_name], 'id': node_dict['id'], 'parent_name': self.parent_name}
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            return []

    @async_atomic(raise_exception=True)
    async def insert(self, node_dict, **kwargs):  # modified dfs
        conn = kwargs['conn']
        if not node_dict[self.parent_name]:
            group_nodes = await self.get_all(limit=1, group_id=node_dict.get(self.group_name, ''), conn=conn)
            if group_nodes:
                raise NodeParentMissing
            else:
                node_dict[self.left_name] = 1
                node_dict[self.right_name] = 2
                del node_dict[self.parent_name]
                print(node_dict, 'node dict')
                new_node = await self._create_node(node_dict, conn=conn)
                return new_node
        parent_node = await self.get_node(node_dict[self.parent_name], conn=conn)
        cur_value = parent_node[self.right_name]
        node_dict[self.left_name] = copy.deepcopy(cur_value)
        node_dict[self.right_name] = copy.deepcopy(cur_value + 1)
        cur_value += 1
        if parent_node[self.parent_name]:
            parent_siblings = await self.get_forward_siblings(parent_node, group_id=parent_node.get(self.group_name), conn=conn)
            if parent_siblings and type(parent_siblings) != list:
                parent_siblings = [parent_siblings]
        else:
            parent_siblings = []
        dfs_stack = Stack()
        for i in parent_siblings:
            dfs_stack.push((True, i, True))
        dfs_stack.push((False, parent_node, True))

        while not dfs_stack.is_empty():  # updating lt,rt entries
            el = dfs_stack.pop()
            if not el[0]:
                cur_value += 1
                node = el[1]
                node[self.right_name] = cur_value
                await self._update_node(node['id'], {self.right_name: node[self.right_name],
                                                     self.group_name:node.get(self.group_name, '')}, conn=conn)
            else:
                node = el[1]
                cur_value += 1
                node[self.left_name] = cur_value
                if await self.has_children(node['id'], conn=conn):
                    dfs_stack.push((False, node, True))
                    children = await self.get_children_by_id(node['id'], conn=conn)  # direct children
                    if children and type(children) != list:
                        children = [children]
                    for i in children:
                        dfs_stack.push((True, i, True))
                else:
                    cur_value += 1
                    node[self.right_name] = cur_value
                await self._update_node(node['id'], {self.right_name: node[self.right_name],
                                                     self.left_name: node[self.left_name],
                                                     self.group_name: node.get(self.group_name, '')}, conn=conn)

            if dfs_stack.is_empty():
                if node[self.parent_name]:
                    parent_node = await self.get_node(node[self.parent_name])
                    if parent_node[self.parent_name]:
                        parent_siblings = await self.get_forward_siblings(parent_node,
                                                                          group_id=parent_node.get(self.group_name,
                                                                                                   conn=conn))
                        if parent_siblings and type(parent_siblings) != list:
                            parent_siblings = [parent_siblings]
                    else:
                        parent_siblings = []
                    for i in parent_siblings:
                        dfs_stack.push((True, i, True))
                    dfs_stack.push((False, parent_node, True))

        new_node = await self._create_node(node_dict, conn=conn)
        return new_node

    def format_col_values_list(self, node_dict):
        values_list = []
        colms_list = []
        for i in node_dict.keys():
            new_str = "'%s'"
            if i == self.group_name and not node_dict[i]:
                continue
            values_list.append(new_str % str(node_dict[i]))
            colms_list.append(str(i))
        return colms_list, values_list

    @async_atomic(raise_exception=True)
    async def _create_node(self, node_dict, **kwargs):  # don't call it manually, node_dict should contain group_id
        conn = kwargs['conn']
        query_string = 'INSERT INTO {table_name} ({colms}) VALUES ({values}) RETURNING *;'
        query_dict = {'table_name': self.table_name,
                      'colms': '',
                      'values': ''}
        colms_list, value_list = self.format_col_values_list(node_dict)
        query_dict['colms'] = ','.join(colms_list)
        query_dict['values'] = ','.join(value_list)
        logger.info(query_string.format(**query_dict))
        result = await conn.fetch(query_string.format(**query_dict))
        if not isinstance(result,list):
            result = [result]
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            raise RowNotCreated

    @async_atomic(raise_exception=True)
    async def _update_node(self, node_id, update_dict, **kwargs):  #node_id is unique group_id not needed
        conn = kwargs['conn']
        query_str = '''
		UPDATE {table_name} SET {set_str} WHERE id='{node_id}' RETURNING *;
		'''
        query_dict = {'table_name': self.table_name, 'node_id': str(node_id),
                      'set_str': ''}
        col, values = self.format_col_values_list(update_dict)
        query_dict['set_str'] = ','.join(("%s=%s" % (i, j) for i, j in zip(col, values)))
        logger.info(query_str.format(**query_dict))
        result = await conn.fetch(query_str.format(**query_dict))
        if result:
            return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
        else:
            return []


class GroupedNestedCategories(NestedCategoriesManager):


    @async_atomic()
    async def _group_check(self, group_id, node_id='', conn=None):
        if not node_id:
            if not group_id:
                raise InvalidArgument("%s is required" % (self.group_name))
        else:
            q_s = '''SELECT * FROM {table} WHERE id='{node_id}';'''.format(table=self.table_name, node_id=node_id)
            res = await conn.fetch(q_s)
            res = RecordHelper.record_to_dict(res, normalize=[uuid_serializer])
            if res[self.group_name] != group_id:
                raise InvalidArgument("%s is required" % (self.group_name))


    async def filter_by_params(self, params, group_id='', **kwargs):
        x = await self._group_check(group_id)
        return await super(GroupedNestedCategories, self).filter_by_params(params, group_id, **kwargs)

    async def get_leaves(self, group_id='', **kwargs):
        x = await self._group_check(group_id)
        return await super(GroupedNestedCategories, self).get_leaves(group_id, **kwargs)

    async def get_all(self, group_id='', limit=sys.maxsize, **kwargs):
        x = await self._group_check(group_id)
        return await super(GroupedNestedCategories, self).get_all(group_id, limit, **kwargs)

    async def get_children(self, parent_id, all=True, group_id='', **kwargs):
        x = await self._group_check(group_id)
        return await super(GroupedNestedCategories, self).get_children(parent_id, all, group_id, **kwargs)

    async def get_forward_siblings(self, node_dict, group_id='', **kwargs):
        x = await self._group_check(group_id)
        return await super(GroupedNestedCategories, self).get_forward_siblings(node_dict, group_id, **kwargs)

    async def insert(self, node_dict, **kwargs):
        grp_id = node_dict.get(self.group_name)
        x = await self._group_check(grp_id)
        return await super(GroupedNestedCategories, self).insert(node_dict, **kwargs)

    async def _create_node(self, node_dict, **kwargs):
        grp_id = node_dict.get(self.group_name)
        x = await self._group_check(grp_id)
        return await super(GroupedNestedCategories, self)._create_node(node_dict, **kwargs)

    async def _update_node(self, node_id, update_dict, **kwargs):
        print(update_dict, 'update dict')
        grp_id = update_dict.get(self.group_name)
        x = await self._group_check(grp_id, node_id)
        return await super(GroupedNestedCategories, self)._update_node(node_id, update_dict, **kwargs)