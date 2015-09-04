import argparse
import sys

class hecuba(object):

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Hecuba Python utility for Database Backup & Restore',
            usage='''hecuba.py <command> [<args>]

The most commonly used hecuba.py commands are:
   backup           Take Cassandra's Snapshot & upload to Amazon S3
   incremental      Take Cassandra's Incremental Backup & upload to Amazon S3
   restore          Restore Cassandra at provided Restore Point
   mysql            Take MySQL Database Dump & upload to Amazon S3
''')
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print 'Unrecognized command'
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def backup(self):
        parser = argparse.ArgumentParser(
            description='Take Cassandra Snapshot & upload to Amazon S3')
        parser.add_argument('keyspaceName', help='Cassandra keyspaceName on which Snapshot will trigger')
        args = parser.parse_args(sys.argv[2:])
        print 'Running Cassandra Snapshot for keyspaceName %s' % args.keyspaceName

    def incremental(self):
        parser = argparse.ArgumentParser(
            description='Take Cassandra Incremental Backup & upload to Amazon S3')
        parser.add_argument('keyspaceName', help='Cassandra keyspaceName on which Incremental Backup will trigger')
        args = parser.parse_args(sys.argv[2:])
        print 'Running Cassandra Incremental Backup for keyspaceName %s' % args.keyspaceName
    
    def restore(self):
        parser = argparse.ArgumentParser(
            description='Restore Cassandra at provided Restore Point')
        parser.add_argument('keyspaceName', help='Cassandra keyspaceName on which Restore will trigger')
        parser.add_argument('restorePoint', help='Restore Point for Keyspace')
        args = parser.parse_args(sys.argv[2:])
        print 'Running Cassandra Restore for keyspaceName %s at restorePoint %s' % (args.keyspaceName, args.restorePoint)
        
    def mysql(self):
        parser = argparse.ArgumentParser(
            description='Take MySQL Dump & upload to Amazon S3')
        parser.add_argument('dbUserName', help='MySQL Database User Name')
        parser.add_argument('dbPassword', help='MySQL Database Password')
        parser.add_argument('dbName', help='MySQL Database Name')
        args = parser.parse_args(sys.argv[2:])
        print 'Running MySQL Dump for Database Name %s as Database UserName %s ' % (args.dbUserName, args.dbPassword, args.dbName)

if __name__ == '__main__':
    hecuba()
    
    




