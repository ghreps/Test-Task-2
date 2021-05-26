import multiprocessing as MP
import multiprocessing.managers as Manager

from logging.handlers import QueueListener, QueueHandler
from datetime import datetime as DT
from time import sleep

import logging
import zipfile
import shutil
import os

STORAGE_DIR = '\\stor\\'
ARCHIVE_DIR = '\\arch\\'
PROCESSES = 2
FORMATTER = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'
logging.basicConfig(level=logging.DEBUG)

def archive(num, queue, lock, log_queue):
    log_name = 'Process ' + str(num)
    logger = logging.getLogger(log_name)
    logger.addHandler(QueueHandler(log_queue))
    logger.debug('Start')
    while True:
        lock.acquire()
        if not queue.empty():
            file = queue.get()
            queue.task_done()
            lock.release()
            logger.info('Archiving ' + file)
            if not os.path.exists(ARCHIVE_DIR + file[:10]):
                os.makedirs(ARCHIVE_DIR + file[:10])
            # Архивируем
            try:
                with zipfile.ZipFile(
                        ARCHIVE_DIR + file + '.zip', 'w', zipfile.ZIP_DEFLATED
                    ) as z:
                    z.write(STORAGE_DIR + file, file[11:])
            except zipfile.BadZipFile:
                logger.error('Fail to zip ' + file)
            else:
                logger.info('...archivated')
                os.remove(file)
                logger.info('Original file deleted')
        else:
            lock.release()
        sleep(1)

def main():
    # Log
    log_queue = MP.Queue(-1)
    file_handler = logging.FileHandler("log.log")
    file_handler.setFormatter(logging.Formatter(FORMATTER))
    queue_listener = QueueListener(log_queue, file_handler)
    queue_listener.start()
    #
    log_name = 'main'
    logger = logging.getLogger(log_name)
    logger.addHandler(QueueHandler(log_queue))
    logger.info('Start')
    #
    manager = Manager.SyncManager()
    manager.start()
    lock = manager.Lock()
    queue = manager.Queue()
    for i in range(PROCESSES):
        p = MP.Process(target=archive, args=(i, queue, lock, log_queue))
        p.start()
    while True:    
        # Пробегаем по всем не пустым папкам и записываем пути
        date_set = set()
        for root, dirs, files in os.walk(STORAGE_DIR, topdown=True):
            for name in files:
                date_set.add(root.replace(STORAGE_DIR, ''))
        if len(date_set) != 0:
            logger.info('Files found - ' + str(len(date_set)))
            # Сортируем по дате от старых к новым
            date_list = []
            for i in date_set:
                date_list.append(DT.strptime(i, '%Y\%m\%d').date())
            date_list.sort()
            # Если осталось менее 10% на харде
            total, used, free = shutil.disk_usage("/")
            if free / total * 100 < 10:
                logger.info('Less 10% free hardspace')
                # Берем самый старый день и перекидываем его файлы в архив
                path = date_list[0].strftime('%Y\%m\%d')
                files = os.listdir(STORAGE_DIR + path)
                for file in files:
                    lock.acquire()
                    queue.put(path + '\\' + file)
                    logger.info('New file in queue - ' + path + '\\' + file)
                    lock.release()
            else:
                for date in date_list:
                    date_difference = DT.now().date() - date
                    if date_difference.days > 90:
                        path = date.strftime('%Y\%m\%d')
                        logger.info('Too old files - ' + path)
                        files = os.listdir(STORAGE_DIR + path)
                        for file in files:
                            lock.acquire()
                            queue.put(path + '\\' + file)
                            logger.info('New file in queue - ' + path + '\\' + file)
                            lock.release()
        else:
            logger.info('No files')
        # Интервал проверки
        sleep(60)
        
if __name__ == '__main__':
    if not os.path.exists(STORAGE_DIR):
        os.mkdir(STORAGE_DIR)
    if not os.path.exists(ARCHIVE_DIR):
        os.mkdir(ARCHIVE_DIR)
    main()