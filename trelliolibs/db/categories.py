import sys
import copy
import uuid
from trelliopg.sql import async_atomic
from trelliolibs.utils.helpers import RecordHelper
from collections import deque

def uuid_serializer(data):
    for key, value in data.items():
        if isinstance(value, uuid.UUID):
            data[key] = str(value)
        if value is None:
            data[key] = ''
    return data

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



class NestedCategoriesManager:

	def __init__(self, table_name, left_col_name, right_col_name):
		self.table_name = table_name
		self.left_name = left_col_name
		self.right_name = right_col_name


	def validate(self, node_dict):
		if self.left_name in node_dict or self.right_name in node_dict:
			return False
		return True

	def get_col_types(self, node_dict):#this will return col:database_col_type dictionary, for insert/update/select query
		types_dict = {}
		for i in node_dict:
			if i in ['id', 'parent_id']:
				types_dict[i] = str
		return types_dict

	@async_atomic(raise_exception=True)
	async def get_leaves(self, *args, **kwargs):
		conn = kwargs['conn']
		query_str = '''SELECT * FROM {table_name}
 					   WHERE {pk} NOT IN (SELECT parent_id FROM {table_name} WHERE parent_id IS NOT null);'''
		query_dict = {'table_name': self.table_name, 'pk':'id'}
		result = await conn.fetch(query_str.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			return []


	@async_atomic(raise_exception=True)
	async def get_all(self, limit=sys.maxsize, **kwargs):
		conn = kwargs['conn']
		query_str = '''SELECT * FROM {table_name} ORDER BY {left_col} LIMIT {limit};'''
		records = await conn.fetch(query_str.format(table_name=self.table_name, limit=limit,
													left_col=self.left_name))
		if records:
			return RecordHelper.record_to_dict(records, normalize=[uuid_serializer])
		else:
			return []


	@async_atomic(raise_exception=True)
	async def has_children(self, node_id, **kwargs):
		conn = kwargs['conn']
		node = await self.get_node(node_id, conn=conn)
		query_str = '''
		SELECT * FROM {table_name} WHERE {parent_name}='{parent_id}' LIMIT 1;
		'''
		result = await conn.fetch(query_str.format(table_name=self.table_name, parent_name='parent_id', parent_id=str(node_id)))
		if result:
			return True
		return False

	@async_atomic(raise_exception=True)
	async def get_children_by_id(self, parent_id, **kwargs):
		conn = kwargs['conn']
		query_str = '''
	    				SELECT * FROM {table_name} WHERE parent_id='{parent_value}'
	    				ORDER BY {left_col};
	    				'''
		query_dict = {'left_col': self.left_name, 'parent_value': str(parent_id),
					  'table_name': self.table_name
					  }
		result = await conn.fetch(query_str.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			return []

	@async_atomic(raise_exception=True)
	async def get_children(self, parent_id=None, all=True, **kwargs):
		conn = kwargs['conn']
		query_str = '''
				SELECT * FROM {table_name} WHERE {left_col}>{left_val} and {right_col}<{right_val}{extra}
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
			query_dict['extra'] = " and parent_id='%s'" % str(parent_id)

		result = await conn.fetch(query_str.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			return []


	@async_atomic(raise_exception=True)
	async def get_node(self, id, **kwargs):
		conn = kwargs['conn']
		query_str = '''
			SELECT * FROM {table_name} WHERE {where_str};
		'''
		query_dict = {'table_name': self.table_name, 'where_str': ''}
		query_dict['where_str'] = "id='%s'" % (str(id))
		result = await conn.fetch(query_str.format(**query_dict))
		if not result:
			raise RowNotFound
		return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])

	@async_atomic(raise_exception=True)
	async def get_forward_siblings(self, node_dict, **kwargs):
		conn = kwargs['conn']
		query_str = '''
		SELECT * FROM {table_name} WHERE parent_id='{parent_value}' and {left_col}>{right_val} and id!='{id}'
		ORDER BY {left_col};
		'''
		query_dict = {'table_name': self.table_name, 'parent_value': node_dict['parent_id'], 'left_col': self.left_name,
					  'right_val': node_dict[self.right_name], 'id': node_dict['id']}
		result = await conn.fetch(query_str.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			return []



	@async_atomic(raise_exception=True)
	async def insert(self, node_dict, **kwargs):#modified dfs
		conn = kwargs['conn']
		if not node_dict['parent_id']:
			if await self.get_all(limit=1, conn=conn):
				raise NodeParentMissing
			else:
				node_dict[self.left_name] = 1
				node_dict[self.right_name] = 2
				del node_dict['parent_id']
				new_node = await self._create_node(node_dict, conn=conn)
				return new_node
		parent_node = await self.get_node(node_dict['parent_id'],  conn=conn)

		cur_value = parent_node[self.right_name]
		node_dict[self.left_name] = copy.deepcopy(cur_value)
		node_dict[self.right_name] = copy.deepcopy(cur_value+1)
		cur_value += 1
		if parent_node['parent_id']:
			parent_siblings = await self.get_forward_siblings(parent_node,  conn=conn)
			if parent_siblings and type(parent_siblings) != list:
				parent_siblings = [parent_siblings]
		else:
			parent_siblings = []
		dfs_stack = Stack()
		for i in parent_siblings:
			dfs_stack.push((True, i, True))
		dfs_stack.push((False, parent_node, True))

		while not dfs_stack.is_empty():#updating lt,rt entries
			el = dfs_stack.pop()
			if not el[0]:
				cur_value += 1
				node = el[1]
				node[self.right_name] = cur_value
				await self._update_node(node['id'], {self.right_name: node[self.right_name]}, conn=conn)
			else:
				node = el[1]
				cur_value += 1
				node[self.left_name] = cur_value
				if await self.has_children(node['id'],  conn=conn):
					dfs_stack.push((False, node, True))
					children = await self.get_children_by_id(node['id'],  conn=conn)#direct children
					if children and type(children) != list:
						children = [children]
					for i in children:
						dfs_stack.push((True, i, True))
				else:
					cur_value += 1
					node[self.right_name] = cur_value
				await self._update_node(node['id'], {self.right_name: node[self.right_name],
														   self.left_name: node[self.left_name]}, conn=conn)

			if dfs_stack.is_empty():
				if node['parent_id']:
					parent_node = await self.get_node(node['parent_id'])
					if parent_node['parent_id']:
						parent_siblings = await self.get_forward_siblings(parent_node, conn=conn)
						if parent_siblings and type(parent_siblings) != list:
							parent_siblings = [parent_siblings]
					else:
						parent_siblings = []
					for i in parent_siblings:
						dfs_stack.push((True,i,True))
					dfs_stack.push((False, parent_node, True))

		new_node = await self._create_node(node_dict, conn=conn)
		return new_node


	def format_col_values_list(self, node_dict, node_types):
		values_list = []
		colms_list = []
		for i in node_dict.keys():
			try:
				if node_types[i] == str:#update for all datatypes
					new_str = "'%s'"
				else:
					new_str = "%s"
			except:
				new_str = "%s"
			values_list.append(new_str % str(node_dict[i]))
			colms_list.append(str(i))
		return colms_list, values_list

	@async_atomic(raise_exception=True)
	async def _create_node(self, node_dict, **kwargs):#don't call it manually
		conn = kwargs['conn']
		query_string = 'INSERT INTO {table_name} ({colms}) VALUES ({values}) RETURNING *;'
		query_dict = {'table_name': self.table_name,
					  'colms': '',
					  'values': ''}
		node_types = self.get_col_types(node_dict)
		colms_list, value_list = self.format_col_values_list(node_dict, node_types)
		query_dict['colms'] = ','.join(colms_list)
		query_dict['values'] = ','.join(value_list)
		result = await conn.fetch(query_string.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			raise RowNotCreated

	@async_atomic(raise_exception=True)
	async def _update_node(self, node_id, update_dict, **kwargs):#don't call it manually
		conn = kwargs['conn']
		query_str = '''
		UPDATE {table_name} SET {set_str} WHERE id='{node_id}' RETURNING *;
		'''
		query_dict = {'table_name': self.table_name, 'node_id': str(node_id),
					  'set_str': ''}
		update_types = self.get_col_types(update_dict)
		col, values = self.format_col_values_list(update_dict, update_types)
		query_dict['set_str'] = ','.join(("%s=%s" %(i,j) for i,j in zip(col, values)))
		result = await conn.fetch(query_str.format(**query_dict))
		if result:
			return RecordHelper.record_to_dict(result, normalize=[uuid_serializer])
		else:
			return []


def nested_categories_factory(table_name,left_col_name='l',right_col_name='r'):
    return NestedCategoriesManager(table_name,left_col_name,right_col_name)