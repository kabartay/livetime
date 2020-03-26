
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
import os.path
import json
import copy
import shutil
import datetime
import collections

def is_dict(x):
  return hasattr(x,"has_key")

# Some functions for nested dictionaries:
def nested_get(ndic,keys):
  if isinstance(keys,list):
    for k in keys: ndic = ndic[k]
    return ndic
  elif isinstance(keys,collections.Hashable): # Just in case...
    return ndic[keys]
def nested_set(ndic,keys,value):
  if isinstance(keys,list):
    if len(keys)>0:
      for key in keys[:-1]:
        ndic = ndic.setdefault(key, {})
      ndic[keys[-1]] = value
      return True
    else: # nested_set(ndic,[],value) -> dic no more... ndic becomes value
      # ndic = value
      return False
  elif isinstance(keys,collections.Hashable): # Just in case...
    ndic[keys] = value
    return True
def nested_keys(ndic,parent=[]):
  if not isinstance(ndic,dict):
    return [tuple(parent)]
  else:
    return reduce(list.__add__,[nested_keys(v,parent+[k]) for k,v in ndic.items()], [])
def nested_exists(ndic,path):
  if isinstance(path,list):
    for k in path:
      if k in ndic:
        ndic = ndic[k]
      else:
        return False
    return True
  elif isinstance(path,collections.Hashable): # Just in case...
    if path in ndic:
      return True
  return False
def nested_find(ndic,key,parent=[],default=None):
  if not isinstance(key,collections.Hashable): # Just in case...
    if isinstance(key,list): # Just in case...
      if nested_exists(ndic,parent+key):
        return nested_get(ndic,parent+key),parent+key,True
    return default,[],False
  if key in ndic: return ndic[key],parent+[key],True
  value,path,found = default,[],False
  for k,v in ndic.items():
    if is_dict(v):
      value,path,found = nested_find(v,key,parent+[k],default)
      if found: return value,path,found
  return value,path,found
def nested_copy(ndic,depth):
  assert depth >= 0
  d = type(ndic)()
  for k, v in ndic.items():
    if is_dict(v):
      if depth > 0:
        d[k] = nested_copy(v,depth-1)
      else:
        # d[k] = None
        # d[k] = u' \u2192 {...} '
        d[k] = ' -> [{...}]'
        # d[k] = {'...':[]}
    else:
      d[k] = v
  return d
def nested_update(d,u,depth=-1):
  for k,v in u.iteritems():
    if is_dict(v) and not depth == 0:
      r = nested_update(d.get(k, {}), v, depth = max(depth - 1, -1))
      d[k] = r
    elif is_dict(d):
      d[k] = u[k]
    else:
      d = {k: u[k]}
  return d

# basic block for the json.db
class jdblock(collections.OrderedDict):
  def __str__(self):
    return json.dumps(self,indent=2)
  def clone(self): # Return an independent copy...
    return copy.deepcopy(self)
  def str(self):
    return self.__str__()
  def df(self,**kwargs):
    import pandas
    kwargs['orient']='index'
    return pandas.DataFrame.from_dict(self,**kwargs)[self.values()[0].keys()]

# A (nested) dictionary like class for a json file use to store permanent
# information, configuration, etc. like a dummy DB
class jsondb(collections.OrderedDict):
  def __init__(self,copyORfile="json.db",**kwargs):
    self.db = jdblock() # To store the DB dict
    self.address = [] # To store the current selected table
    self.subdb = self.db # To point directly to the DB selected section
    self.meta = jdblock() # Some information...
    self.file = None # To store the DB file name
    self.master = True # If the jsondb is a copy of other this is set to False
    if isinstance(copyORfile, jsondb): # Copy...
      self.db = copyORfile.db                      # Same DB...
      self.address = copy.copy(copyORfile.address) # ...but independent address pointer
      self.subdb = copyORfile.subdb
      self.meta = copyORfile.meta
      self.file = copyORfile.file
      self.master = False
    elif isinstance(copyORfile, str): # Load...
      if os.path.isfile(copyORfile):
        self.load(copyORfile,**kwargs)
      else:
        self.meta["name"] = copyORfile
    elif is_dict(copyORfile): # Initialize... self.file not defined
      self.db = jdblock(copyORfile)
      self.subdb = self.db
    else:
      print " > jsondb.ERROR: Wrong type of variable, not a [jsondb,str,dict] type... [{0}:'{1}']".format(type(copyORfile),copyORfile)
    self.meta["date"] = "{:%Y/%m/%d %H:%M:%S.%s}".format(datetime.datetime.now())
  def copy(self): # Return a copy...
    return jsondb(self)
  def clone(self): # Return an independent copy...
    return copy.deepcopy(self)
  def load(self,jsonfile,**kwargs): # Load DB from a file...
    self.file = jsonfile
    self.address = []
    try:
      self.db = json.load(open(jsonfile),object_pairs_hook=jdblock)
    except: # Not JSON format, but a simple ASCII column file, with first line as a header
      nline = 0
      dkwargs = {"header":False,"columns":False}
      dkwargs.update(kwargs)
      itemdb = jdblock()
      f = open(jsonfile,'r')
      for nline,line in enumerate(f):
        if nline == 0:
          if dkwargs["header"]:
            dkwargs["columns"] = line.split()
            continue
          elif not dkwargs["columns"]:
            dkwargs["columns"] = range(0,len(line.split()))
        for nitem,item in enumerate(line.split()):
          itemdb[dkwargs["columns"][nitem]] = item
        self.db[nline] = itemdb.copy()
      f.close()
      self.meta = {"ascii":"Loaded from ASCII file \"FILE\" with columns \"COLUMNS\"".replace('FILE',jsonfile).replace('COLUMNS',str(dkwargs["columns"]))}
    if "meta" in self.db and "data" in self.db:
      self.meta.update(self.db["meta"])
      self.db = self.db["data"]
    self.subdb = self.db
  def backup(self,filename=None): # Create a backup copy of the DB file...
    if filename is None:
      filename = self.file
    if os.path.isfile(filename):
      return shutil.copy2(filename,"{0}".format(filename)+"{:.%Y_%m_%d__%H_%M_%S.%s}".format(datetime.datetime.now()))
    return False
  def save(self,newfile=None): # Save the DB into a file...
    if newfile is None:
      if self.file is None:
        print " > jsondb.ERROR: File to save not given..."
        return False
      newfile = self.file
    elif self.file is None:
      self.file = newfile
    self.backup(newfile)
    json.dump(jdblock({"meta":self.meta,"data":self.db}),open(newfile,'w'),indent=2)
    return True
  def info(self,newkey=None,newinfo=None): # Handle meta info...
    if newkey is not None:
      if newinfo is None:
        if is_dict(newkey): self.meta.update(newkey)
        else: print " > jsondb.ERROR: Information is not of a valid type... [{0}:'{1}']".format(type(newkey),newkey)
      else: self.meta[newkey] = newinfo
    return self.meta
  def setpath(self,key=None): # To set the address pointer within the DB...
    # jsondb.setpath("cosa") -> searched
    # jsondb.setpath(["levelA","levelB","cosa"])
    # jsondb.setpath([]) = jsondb.setpath()
    if key is None or key == []: # Reset to db root
      self.address = []
      self.subdb = self.db
      return True
    if isinstance(key,list): # Set address to this path
      if nested_exists(self.subdb,key):
        self.subdb = nested_get(self.subdb,key)
        self.address += key
        return True
      return False
    if isinstance(key,collections.Hashable): # Look for the key somewhere in the DB...
      value,path,found = nested_find(self.subdb,key)
      if found:
        self.subdb    = value
        self.address += path
      return found
    return False
  def setvalue(self,keysORvalue,value=None): # To set the value of a certain key in the DB...
    # jsondb.setvalue("cosa",value)
    # jsondb.setvalue(["levelA","levelB","cosa"],value)
    # jsondb.setvalue([],value) = jsondb.setvalue(value)
    if value is not None:
      if isinstance(keysORvalue,list):
        if len(keysORvalue)>0:
          nested_set(self.subdb,keysORvalue,value)
          return True
        else: # Do like jsondb.setvalue(value)
          keysORvalue = value
      else: # Classical dict...
        if isinstance(keysORvalue,collections.Hashable):
          self.subdb[keysORvalue] = value
          return True
        else:
          print " > jsondb.ERROR: Not hashable key provided... [{0}:'{1}']".format(type(keysORvalue),keysORvalue)
          return False
    # I want set the db at the level of subdb with keysORvalue
    if len(self.address)<1: # self.address = [] -> We are at the base of the DB
      if is_dict(keysORvalue):
        self.db = jdblock(keysORvalue)
        self.subdb = self.db
        return True
      else:
        print " > jsondb.ERROR: Attempt to set DB to a non dictionary... [{0}:'{1}']".format(type(keysORvalue),keysORvalue)
        return False
    nested_set(self.db,self.address,keysORvalue)
    return True
  def getpath(self,key=None,absolute=True):
    if key is None: return self.address
    if absolute:    return self.address + nested_find(self.subdb,key)[1]
    else:           return nested_find(self.subdb,key)[1]
  def getvalue(self,key=None):
    if key is None: return self.subdb
    return nested_find(self.subdb,key)[0]
  def exists(self,key):
    return nested_find(self.subdb,key)[2]
  def add(self,nameORthing,fileORdict=None,**kwargs): # Add a json.db file to the DB with in the provided key name at the address level
    # jsondb.add("./file.db.json") # It uses "file" as key name
    # jsondb.add("new_key","./file.db.json") # It uses "new_key" as key name
    # jsondb.add("new_key",{"sample":True,"more":False}) # It uses "new_key" as key name
    # jsondb.add({"sample":True,"more":False}) # It inserts the fields at the given level
    if fileORdict is None:
      if "depth" in kwargs:
        nested_update(self.subdb,jsondb(nameORthing).db,kwargs["depth"])
      else:
        self.subdb.update(jsondb(nameORthing).db)
    else:
      ndic = {}
      nested_set(ndic,nameORthing,jsondb(fileORdict).db)
      if "depth" in kwargs:
        nested_update(self.subdb,ndic,kwargs["depth"])
      else:
        self.subdb.update(ndic)
  def drop(self,key=None):
    # jsondb.drop("Options") # It removes that key
    # jsondb.drop() # It removes the key to which aims self.address
    if key is None:
      if len(self.address)<1: # self.address = [] -> We are at the base of the DB... drop this is the same as a reset
        self.db = {}
        self.subdb = self.db
      else:
        del self[self.address]
        self.address = self.address[:-1]
        self.subdb = nested_get(self.db,self.address)
      return True
    value,path,found = nested_find(self.subdb,key)
    if found:
      if len(path)>0: subset = nested_get(self.subdb,path[:-1])
      else:           subset = self.subdb
      if isinstance(key,list): key = key[-1]
      del subset[key]
      return True
    return False
  def export(self,filename,key=None):
    # jsondb.export("./file.db.json","Settings") # It exports the key to that db.json file
    # jsondb.export("./file.db.json",["Settings","Inputs"]) # It exports the key to that db.json file... in theory
    # jsondb.export("./file.db.json") # It exports the current address to that db.json file
    if key is None:
      subset = self.copy().setpath(self.address)
      subset = jsondb(subset.subdb)
      subset.save(filename)
      return True
    elif isinstance(key,collections.Hashable) or isinstance(key,list): # Look for the key somewhere in the DB...
      if not isinstance(key,list): key = [key]
      value,path,found = nested_find(self.subdb,key)
      if found:
        subset = self.copy()
        subset.setpath(path)
        subset = jsondb(subset.subdb)
        subset.save(filename)
        return True
      print " > jsondb.ERROR: Key '{0}' not found in [ {1} ]".format(key,u' \u2192 '.encode('utf-8').join(['DB']+self.address))
      return False
    print " > jsondb.ERROR: Not hashable key provided... [{0}:'{1}']".format(type(key),key)
    return False
  def set(self,key=None): # Most of the time is setpath...
    # jsondb.set("Options")
    if self.setpath(key):
      print "Path setted to [ {} ]".format((u' \u2192 '.encode('utf-8')).join(['DB']+self.address))
      return True
    else:
      print "Path '{0}' not found in [ {1} ]".format(key,u' \u2192 '.encode('utf-8').join(['DB']+self.address))
      return False
  def get(self,key=None):
    # subset = jsondb.get("Options") # It returns a jsondb object with setpath there
    # subset = jsondb.get("Options").clone() # It returns an independent new jsondb object with setpath there
    subset = self.copy()
    subset.set(key)
    return subset
  # http://www.siafoo.net/article/57
  def __call__(self,key=None): # A complex overloaded method... shame of me
    # Tipycally returns a referenced copy with address set to the given key without modifying its address but
    # if it has been loaded from a file or created from zero (jsondb is not a copy) it doesn't modify its addres.
    # If a dict is passed, the current address is setvalue() with the dict (to update use .add())
    if is_dict(key):
      self.setvalue(key)
      return self.get()
    if not self.master: self.set(key)
    return self.get(key)
  def __setitem__(self,key,value): # jsondb["par1"]=value1 at self.adress level
    self.setvalue(key,value)
    # self.subdb[key] = value
  def __getitem__(self,key): # jsondb["par1"] at self.adress level
    return self.getvalue(key)
    # return self.subdb[key]
  # http://farmdev.com/src/secrets/magicmethod/
  # def __setattr__(self, key, value): # jsondb.par1=value1
  #   self.subdb.__setattr__(self, key, value)
  # def __getattribute__(self, key): # jsondb.par1
  #   return self.subdb[key]
  def __delitem__(self, key):
    self.drop(key)
    # del self.subdb[key]
  def keys(self):
    return self.subdb.keys()
  def values(self):
    return self.subdb.values()
  def items(self):
    return self.subdb.items()
  def __contains__(self, item):
    return item in self.subdb
  def __iter__(self):
    return iter(self.subdb)
  def __unicode__(self):
    return unicode(repr(self.subdb))
  def __cmp__(self, dict):
    return cmp(self.subdb, dict)
  def __str__(self):
    return self.subdb.__str__()
    # return nested_copy(self.subdb,0).__str__()
  def str(self):
    return self.__str__()
  def dict(self):
    return self.subdb
  def df(self,**kwargs):
    return self.subdb.df()


# Return a dict with keys from a given list and a new default argument for each value
# If the default argument is an empty object [] or {} an independent new one is created for each key
# newdict = list2dict(["key1","key2"]) -> default = None
# newdict = list2dict(["key1","key2"],default=[]) // An independent clone for each field
# newdict = list2dict(["key1","key2"],default={}) // An independent clone for each field
# newdict = list2dict(["key1","key2"],default=jdblock{}) // An independent clone for each field
# newdict = list2dict(["key1","key2"],default="unset")
def list2dict(alist,**kwargs):
  adict = jdblock()
  if isinstance(alist,list):
    for ele in alist:
      if isinstance(ele,collections.Hashable):
        if "default" in kwargs:
          if kwargs["default"] == []:        adict[ele] = []
          if kwargs["default"] == {}:        adict[ele] = {}
          if kwargs["default"] == jdblock(): adict[ele] = jdblock()
          else:                              adict[ele] = kwargs["default"]
        else:                                adict[ele] = None
  return adict

# Return a dict shallow copy without the given keys
# newdict = dropkeys(adict,"key1")
# newdict = dropkeys(adict,"key1","key2")
# newdict = dropkeys(adict,["key1","key2"])
# newdict = dropkeys(adict,keys=["key1","key2"])
def dropkeys(adict,*keys,**kwargs):
  if "keys" in kwargs: keys = kwargs["keys"]
  if len(keys)>0 and isinstance(keys[0],list): keys = keys[0]
  newdict = copy.copy(adict)
  for key in adict.keys():
    if key in keys:
      del newdict[key]
  return newdict

# Return a list with duplicated elements removed
def list2unique(alist):
  return list(collections.OrderedDict.fromkeys(alist))

# Return a dict shallow copy with only the given keys and in the given order
# newdict = dropkeys(adict,["key1","key2"])
# newdict = dropkeys(adict,keys=["key1","key2"])
# newdict = dropkeys(adict,order=["key1","key2"])
def orderdict(adict,*keys,**kwargs):
  if len(keys)>0 and isinstance(keys[0],list): keys = keys[0]
  elif "keys"  in kwargs: keys = kwargs["keys"]
  elif "order" in kwargs: keys = kwargs["order"]
  newdict = jdblock()
  for key in keys:
    if key in adict.keys():
      newdict[key] = adict[key]
  return newdict

# It return a dict of dicts using kwargs as key = value for each element in kwargs["header"] (or the first one)
# considering kwargs["order"] if defined, so keys "header" and "order" are reserved...
# table = dotable(
#   header = ["PSRB125963","HESSJ0632057","a1FGLJ101865856","LS503963","CygX3"],
#   order  = ["name","dec","ra"],
#   name   = ["PSR B1259-63","HESS J0632+057","1FGL J1018.6-5856","LS 5039-63","Cyg X-3"],
#   dec    = [-63.831,5.806,-58.945,-14.825,40.958],
#   ra     = [195.705,98.243,154.742,276.563,308.107],
#   other  = [195.705,98.243,154.742,276.563,308.107],
# )
def dotable(**kwargs):
  table = jdblock()
  if len(kwargs) > 0:
    if "order" in kwargs:
      nkwargs = orderdict(kwargs,kwargs["order"])
      for key in kwargs: nkwargs[key] = kwargs[key] # Cheap trick to keep also keys not in "order"...
      kwargs = dropkeys(nkwargs,["order"])
    if "header" in kwargs:
      header = kwargs["header"]
      kwargs = dropkeys(kwargs,["header"])
    else:
      header = range(0,len(kwargs.values()[0]))
    table = list2dict(header,default=jdblock())
    for idx,hkey in enumerate(table.keys()):
      for key in kwargs.keys():
        try:
          table[hkey][key] = kwargs[key][idx]
        except:
          table[hkey][key] = None
  return table


# Doing safe names:
def safenames(names,subs=[['+','p'],['-','n'],' ','.'],**kwargs):
  dkwargs = {"alpha":True}
  dkwargs.update(kwargs)
  if isinstance(names,list): newnames = list(names)
  else:                      newnames = [names]
  for idx,name in enumerate(newnames):
    if isinstance(name,str):
      if not name[:1].isalpha() and dkwargs["alpha"]: name = "a"+name
      for sub in subs:
        if isinstance(sub,list):
          if len(sub)>1: name = name.replace(sub[0],sub[1])
          else:          name = name.replace(sub[0],'')
        else:            name = name.replace(sub,'')
    newnames[idx] = name
  if isinstance(names,list): return newnames
  else:                      return newnames[0]
