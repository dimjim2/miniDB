class HashTable(object):

    def __init__(self):
        """Initialize this HashTable and set items if specified"""
        #starting with 7 slots
        self.slots = [[] for _ in range(7)]
        self.size = 0

    #display method of HashTable
    def display_hash(self):
        for i in range(len(self.slots)):
            print(i, end=" ")

            for j in self.slots[i]:
                print("-->", end=" ")
                print(j, end=" ")

            print()

    """Return a hash index by hashing the key and finding the remainder of the hash
    divided by the number of slots in the HashTable"""
    #hash function
    def _hash_function3(self,key_str, size):
        return sum([ord(c) for c in key_str]) % size
    #hash function only for integers keys
    def _hash_function4(self,key,size):
         return key%size

    """Hashing Visualisation Tool.We use graphviz package which
    facilitates the creation and rendering of graph descriptions.In this function
    we make a graph that contains all slots numbered(eg 2 slot means third slot)
    If a slot is not empty all values that are insided are connected with the slot
    using edges (like linkedlist).In the end all buckets with their values
    displayed in a graphical way.
    """
    def plot(self):

        # add each node and each link
        g = 'digraph G{\nforcelabels=true;\n'
        count=100
        for i,kv in enumerate(self.slots):
            #make slot node
            g+=f'{i} [label="{i}"]\n'
            for j,l in enumerate(kv):
                key, value = l
                print("key ",key )
                print("value ", value)
                #make node for hashes
                g+=f'{count} [label="{l}"]\n'

                #make edges
                if(j!=0):

                    g+=f'{count-1}->{count};\n'
                else:
                    g+=f'{i}->{count};\n'
                count+=1
            g+='\n'


        g +="}"
        try:
            from graphviz import Source
            src = Source(g)
            src.render('Hash', view=True)
        except ImportError:
            print('"graphviz" package not found. Writing to graph.gv.')
            with open('graph.gv','w') as f:
                f.write(g)


    def _get_hash_index(self, key):
        #return self._hash_str(key) % len(self.slots)
        if not((isinstance(key,int))):
            return self._hash_function1(key,len(self.slots))
        else:
            #convert int to str and use different hash function(we do because it it more efficient than use _hash_function3)
            return self._hash_function1(str(key),len(self.slots))



    def _hash_function1(self, string,size):
        """Return a hash of the given string.Djb2 hash function """
        hash = 5381
        for char in string:
            hash = (((hash << 5) + hash) + ord(char))
        return hash % size
    def _hash_function2(self, string,size):
        """Return a hash of the given string.Sdbm hash function"""
        hash = 0
        for char in string:
            hash = (hash << 6) + (hash << 16) - hash + ord(char)
        return hash % size

    def get(self, key_search):
        """Return list with values found by given key in the HashTable,if key not found
        raise exception"""
        list_return=[]
        count=0

        # get the slot the key belongs to
        # using our _get_hash_index function
        slot = self.slots[self._get_hash_index(key_search)]
        #print("slot ",slot," len ",len(slot))
        for i,kv in enumerate(slot):
            #print("the number of slot ", i, " the slot contains ", kv)
            key, value = kv
            #print("key ", key)
            #print("value ", value)
            count+=1
            if key == key_search:
                list_return.append(value)
        if(len(list_return)==0):
            raise Exception("Hash table does not contain key.")
        else:
            print(f'With Hash -> {count} comparison operations needed')
            return list_return


    def set(self, key, value):
        """Add an item to the HashTable by key and value"""

        # get the slot where the key belongs to
        # using our _get_hash_index function
        slot = self.slots[self._get_hash_index(key)]
        self.size += 1

        # append (key,value) to the end of the slot
        slot.append((key,value))

        # if load factor exceeds 0.95, resize
        '''
        if (self.size / len(self.slots)) > 0.95:
            self._resize()
        '''

    def delete(self, key_search):
        """Remove an item from the HashTable by key or print message key not found """

        # get the slot the key belongs to
        # using our _get_hash_index function
        bucket = self.slots[self._get_hash_index(key_search)]
        key_exists=False
        indexes_del=[]

        # delete item or throw key error if item was not found
        for i, kv in enumerate(bucket):
            key, v = kv
            if key == key_search:
                key_exists = True
                indexes_del.append(i)
        #indexes_del=indexes_del.reverse()
        if key_exists:
            for i in indexes_del:
                bucket[i]="None"
                #del bucket[i]
            for i in range(len(bucket)-1,-1,-1):
                if bucket[i]=="None":
                    del bucket[i]



            print('Key {}  deleted'.format(key_search))
        else:
            print('Key {} not found'.format(key_search))

    def _resize(self):
        """"Resize the HashTable by doubling the number of slots and rehashing all items"""

        # get a list of all items in the hash table
        l=self.slots

        # reset size for hash table
        self.size = 0

        # generate new slots of double current slots
        self.slots = [[] for i in range(len(self.slots) * 2)]
        for i in range(len(l)):
            for _,m in enumerate(l[i]):
                k, v = m
                self.set(k,v)
        return self.slots


"""
H=HashTable()
H.set("Arts",90)
H.set("Computer Science",12)
H.set("Literature",11)
H.set("Physics",12)
H.set("Biology",11)
H.set("Computer Science",5)
H.set("Literature",9)
H.set("Physics",3)
H.set("8",9)
H.display_hash()
print(H.get("Biology"))
H.delete("Physics")
print(H.slots)
"""
