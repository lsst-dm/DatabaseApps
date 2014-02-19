import xml.parsers.expat

class Xmlslurper:

    def __init__(self, filename, tablenames):
        
        self.data = {}

	def start_element(name, attrs, data = self.data):
            if "TABLE" == name:
                #print "TABLE name=",  attrs['name']
                if not attrs['name'] in self.data['wanted_tables']:
                    return
                data['curtable'] = attrs['name']
                data['fieldnames'] = []
                data['fieldtypes'] = []
                data['fieldarray'] = []
                data['tables'][data['curtable']] = {}
            if "FIELD" == name and self.data['curtable']:
                #print "FIELD name=",   attrs['name']
                data['fieldnames'].append(attrs['name'].lower())
                data['fieldtypes'].append(attrs['datatype'])
                data['fieldarray'].append(attrs.get('arraysize',None))
            if "TR" == name:
                data['col'] = 0
            if "TD" == name:
                data['in_TD'] = True

	def end_element(name, data = self.data):
            if "TD" == name:
                data['col'] += 1
                data['in_TD'] = False
            if "TABLE" == name and data['curtable']:
                data['curtable'] = None
		del self.data['fieldnames'] 
		del self.data['fieldtypes']
		del self.data['fieldarray']

	def char_data(text, data = self.data):
            if data['in_TD'] and self.data['curtable']:
               #print "TD data for ", data['fieldnames'][data['col']] , " is ", data
               if data['fieldarray'][data['col']] != None  and data['fieldtypes'][data['col']] != 'char':
                   vals = text.strip().split()
                   #print "splits to:" , vals
		   if data['fieldtypes'][data['col']] == 'int':
                        for i in range(0,len(vals)):
                            vals[i] = int(vals[i])
		   elif data['fieldtypes'][data['col']] == 'float':
                        for i in range(0,len(vals)):
                            vals[i] = float(vals[i])
		   data['tables'][data['curtable']][data['fieldnames'][data['col']]] = vals
               else:
		   if data['fieldtypes'][data['col']] == 'int':
		       text = int(text)
		   if data['fieldtypes'][data['col']] == 'float':
		       text = float(text)
		   data['tables'][data['curtable']][data['fieldnames'][data['col']]] = text

        self.data['wanted_tables'] = tablenames
        self.data['curtable'] = None
        self.data['params'] = {}
        self.data['tables'] = {}
	self.data['in_TD'] = False

        p = xml.parsers.expat.ParserCreate()
        p.StartElementHandler = start_element
        p.EndElementHandler = end_element
        p.CharacterDataHandler = char_data

        f = open(filename,"r")
        p.ParseFile(f)
        f.close()
        #
        # clean out our bookkeeping
        #
	del self.data['curtable'] 
	del self.data['wanted_tables'] 
        del self.data['in_TD']

    def gettables(self):
        return self.data['tables']

    #
    # we look like our data member...
    #
    def __getattr__(self,blah):
        return getattr(self.data['tables'], blah)

if __name__ == "__main__":
    tablelist = (
	    "Astrometric_Instruments",
	    "FGroups",
	    "Fields",
	    "Photometric_Instruments",
	    "PSF_Extensions",
	    "PSF_Fields",
	    "Warnings")
    import glob
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    for f in glob.glob('*.xml'):
        print "f: ", f
        pp.pprint(Xmlslurper(f,tablelist).gettables())
