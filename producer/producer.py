import datetime
import random
import sys
import time
from kafka import KafkaProducer

class Producer:
    def __init__(self, args, createProducer = True):
        self.config={}
        self.get_app_config(args=args)
        if not self.config['error'] and createProducer:
            self.producer = self.configure_producer()
    
    def configure_producer(self):
        producer_config = {
            'bootstrap_servers': '0.0.0.0:9094',
            'client_id': self.config['client_id'],
            'acks': 'all',
            'linger_ms': 10,
        }
        return KafkaProducer(**producer_config)

    @staticmethod
    def get_date_timestamp(start_time, seconds):
        to_ret = start_time + datetime.timedelta(seconds=seconds)
        return to_ret.strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def get_random_date():
        return datetime.datetime.now() - \
            datetime.timedelta(days=random.randint(0, 365),
                                hours=random.randint(0, 24),
                                minutes=random.randint(0, 60),
                                seconds=random.randint(0, 60))

    def get_app_config(self, args):
        start_path = '/Users/danilomilosevic/Documents/Danilo/VS/'
        self.config = {
            'client_id':'emission-producer',
            'group':'emission-group',
            'to_print':False,
            'sleep_time':0,#s
            'train_topic':'ems_train_topic',
            'pred_topic':'ems_pred_topic',
            'file':start_path + 'emissions.csv',
            'error':False,
            'split_train_pred': 5000
        }

        if len(args) < 2:
            print("\tUsage: python3 producer.py [print|noprint] [sleep_time(ms)]")
            self.config['error'] = True
            return
        
        try:
            self.config['to_print'] = args[1] == 'print'
            try:
                self.config['sleep_time'] = float(args[5])/1000.0
            except ValueError:
                self.config['error'] = True
                print('\tSleep time has to be in milliseconds!')
        except:
            pass

    def format_line(self, line, start_date=None):
        seconds = float(line.split(";")[0])
        return (Producer.get_date_timestamp(start_date, seconds) + line[line.index(";"):],True)
        
    def produce_records(self):
        if(self.config['error']):
            print('\tError in config!')
            return
        curr = 0
        with open(self.config['file']) as file:
            line = file.readline() #skip first line
            start_date = Producer.get_random_date()
            line = file.readline()
            while line:
                processed_line, ok = self.format_line(line, start_date=start_date)
                if ok:
                    if self.config['to_print']:
                        print('Sending to topic: ', processed_line)
                    self.producer.send(
                        topic = self.config['train_topic'] if curr < self.config['split_train_pred'] else self.config['pred_topic'],
                        value = processed_line.strip().encode('utf-8'))
                    self.producer.flush()
                    if self.config['to_print']:
                        print('\tSent!')
                    curr+=1
                time.sleep(self.config['sleep_time'])
                line = file.readline()
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    p = Producer(sys.argv)
    p.produce_records()
    print("Done!")
