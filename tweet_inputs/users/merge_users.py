import os ; import sys ; import pickle

def main():
    num_partitions = 8
    unique_users = {}
    print "Merging..."
    for filename in os.listdir("."):
        if filename[filename.rfind('.')+1:] == 'pickle':
            f = open(filename, 'rb')
            users = pickle.load(f)
            f.close()
            print len(users)
            for user in users:
                unique_users[user['id']] = user
    print "Unique users: %s"%(len(unique_users))
    print "Partitioning..."
    partition_size = len(unique_users) / num_partitions
    for i in range(num_partitions):
        f_unique_users = open('unique/%s.pickle'%(i), 'wb')
        pickle.dump(unique_users.values()[i*partition_size:(i+1)*partition_size], f_unique_users)
        f_unique_users.close()

    fw = open('new_geo_users.txt', 'wb')
    for uid in unique_users.iterkeys():
        fw.write(uid.encode('UTF-8')+'\n'))
    fw.close()

    
if __name__ == '__main__':
    main()