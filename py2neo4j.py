import neo4j

#use cypher statements to manuplate neo4jdb-backend network graph data
class Node:
	def __init__(self,*labels,**attrs):
		self.labels=labels
		self.attrs=attrs
		self.index=None
		try:
			self.id=self.attrs['id']
		except:
			raise Exception('No Identifier Defined For Given Node!')

	def set_index(self,index):
		self.index=index
	@property
	def labels(self):
		return self.labels
	@property
	def id(self):
		return self.id
	@property
	def attrs(self):
		return self.attrs
class Relation:
	def __init__(self,*triple,**attrs):
		try:
			self.start_node,self.relation_type,self.end_node=triple
		except:
			raise Exception('Relation Length Must Be Three With Double Nodes And A Tagged Relation Type!')
		try:
			self.id=attrs['id']
		except:
			raise Exception('No Identifier Defined For Given Relation!')
		self.attrs=attrs
		self.index=None
		if attrs.has_key('bidirectional'):
			self.bidirectional=attrs['bidirectional']
		else:
			self.bidirectional=False
	def set_index(self,index):
		self.index=index
	@property
	def start_node(self):
		return self.start_node
	@property
	def end_node(self):
		return self.end_node
	@property
	def relation_type(self):
		return self.relation_type
	@property
	def id(self):
		return self.id
	@property
	def attrs(self):
		return self.attrs
	@property
	def bidirectional(self):
		return self.bidirectional
class Neo4jConnection(neo4j.connection.Connection):
	def __init__(self,dsn='http://localhost:7474/db/data/'):
		super(Neo4jConnection,self).__init__(dsn)
		self.cursor=self.cursor()

	#return the number of nodes in graph db
	@property
	def order(self):
		statement='MATCH (n) RETURN count(n)'
		return self.cursor.execute(statement).next()[0]

	#return the number of relations in graph db
	@property
	def size(self):
		statement='MATCH ()-[r]-() RETURN count(r)'
		return self.cursor.execute(statement).next()[0]
		
	#to delete target
	def delete_one_node(self,node):
		statement="MATCH (n) WHERE id(n)=%s DELETE n " % node.index
		self.cursor.execute(statement)
		return node

	def delete_nodes(self,*node):
		for node in nodes:
			yield self.delete_one_node(node)

	def delete_one_relation(self,relation):
		statement="MATCH ()-[r]-() WHERE id(r)=%s DELETE r" % relation.index
		self.cursor.execute(statement)
		return relation

	def delete_relations(self,*relations):
		for relation in relations:
			yield self.delete_relation(relation)

	def delete_all(self):
		print 'Warning: All Nodes And Relations Will Be Permanently Removed!'
		print 'Deleting All Edges...'
		statement='MATCH ()-[r]-() DELETE r'
		self.cursor.execute(statement)
		print 'Deleting All Nodes...'
		statement='MATCH (n) DELETE n'
		self.cursor.execute(statement)
		print 'Done!'
		print

	def create_one_node(self,node):
		if not isinstance(node,Node):
			raise Exception('Invalid Parameter! Node Type Enssential!')
		labels=':'.join(node.labels)
		attrs={'node':node.attrs}
		statement="CREATE (n:%s {node}) RETURN id(n),labels(n),n" % labels
		node_index,node_labels,node_attrs=self.cursor.execute(statement,**attrs).next()
		node.set_index(node_index)
		return node

	def create_nodes(self,*nodes):
		for node in nodes:
			yield self.create_one_node(node)

	def merge_one_node(self,node):
		if not isinstance(node,Node):
			raise Exception('Invalid Parameter! Node Type Enssential!')
		labels=':'.join(node.labels)
		attrs={'node':node.attrs}
		statement="MERGE (n:%s {%s}) RETURN id(n),labels(n),n" % (labels,','.join(map(lambda x:x[0]+':{'+attrs.keys()[0]+'}.'+x[0],node.attrs.items())))
		node_index,node_labels,node_attrs=self.cursor.execute(statement,**attrs).next()
		node.set_index(node_index)
		return node

	def merge_nodes(self,*nodes):
		for node in nodes:
			yield self.merge_one_node(node)

	def _create_one_relation(self,relation,unique=False):
		if not isinstance(relation,Relation):
			raise Exception('Invalid Parameter! Node Type Enssential!')
		start_node,end_node,relation_type,is_bidirectional=relation.start_node,relation.end_node,relation.relation_type,relation.bidirectional
		start_node_labels,end_node_labels=map(lambda x:':'.join(x.labels),[start_node,end_node])
		start_node_index,end_node_index=start_node.index,end_node.index
		relation_attrs={'relation':relation.attrs}
		statement='MATCH (start_node:%s),(end_node:%s) WHERE id(start_node)=%s AND id(end_node)=%s' % (start_node_labels,end_node_labels,start_node_index,end_node_index)
		if unique:
			statement+=' CREATE UNIQUE'
		else:
			statement+=' CREATE'
		if is_bidirectional:
			statement+=' (start_node)-[r:%s {relation}]-(end_node)' % relation_type
		else:
			statement+=' (start_node)-[r:%s {relation}]->(end_node)' % relation_type
		statement+=' RETURN id(r),type(r),r'
		relation_index,relation_type,relation_attrs=self.cursor.execute(statement, **relation_attrs).next()
		relation.set_index(relation_index)
		return relation

	def create_one_relation(self,relation):
		return self._create_one_relation(relation)

	def create_unique_one_relation(self,relation):
		return self._create_one_relation(relation,unique=True)

	def create_relations(self,*relations):
		for relation in relations:
			yield self.create_one_relation(relation)

	def create_unique_relations(self,*relations):
		for relation in relations:
			yield self.create_unique_one_relation(relation)

	#to find target nodes
	def find(self,*labels,**attrs):
		if not labels:
			raise Exception('Empty Labels Invalid!')
		labels=':'.join(labels)
		if attrs:
			attrs={'node':attrs}
			statement='MATCH (node:%s {node})' % labels
			statement+=' RETURN id(node),labels(node),node'
			result=self.cursor.execute(statement,**attrs)
		else:
			statement='MATCH (node:%s)' % labels
			statement+=' RETURN id(node),labels(node),node'
			result=self.cursor.execute(statement)
		for node_index,node_labels,node_attrs in result:
			node=Node(*node_labels,**node_attrs)
			node.set_index(node_index)
			yield node

	#to find target node
	def find_one(self,*labels,**attrs):
		return self.find(limit=1,*labels,**attrs).next()

	#to match target relations
	def match(self,start_node=None,rel_type=None,end_node=None,bidirectional=True,**attrs):
		statement=''
		relation_attrs={'relation':attrs}
		if not start_node and not end_node:
			statement+='MATCH (start_node)'
		elif not end_node:
			statement+='MATCH (start_node) WHERE id(start_node)=%s' % start_node.index
		elif not start_node:
			statement+='MATCH (end_node) WHERE id(end_node)=%s' % end_node.index
		else:
			statement+='MATCH (start_node) WHERE id(start_node)=%s MATCH (end_node) WHERE id(end_node)=%s' % (start_node.index,end_node.index)
		rel_clause=''
		if rel_type:
			rel_clause+=':'+rel_type
		rel_clause+=' '
		if attrs:
			rel_clause+='{%s}' % ','.join(map(lambda x:x[0]+':{'+relation_attrs.keys()[0]+'}.'+x[0],attrs.items()))
		if bidirectional:
			statement+=' MATCH (start_node)-[relation'+rel_clause+']-(end_node)'
		else:
			statement+=' MATCH (start_node)-[relation'+rel_clause+']->(end_node)'
		statement+=' RETURN id(relation),type(relation),start_node,end_node,relation'
		result=self.cursor.execute(statement,**relation_attrs)
		for relation_index,relation_type,start_node,end_node,relation_attrs in result:
			relation=Relation(*(start_node,relation_type,end_node),**relation_attrs)
			relation.set_index(relation_index)
			yield relation

	#to match target relation
        def match_one(self,start_node=None,rel_type=None,end_node=None,bidirectional=True,limit=None,**attrs):
                return self.match(start_node=start_node,rel_type=rel_type,end_node=end_node,bidirectional=bidirectional,limit=limit,**attrs).next()
