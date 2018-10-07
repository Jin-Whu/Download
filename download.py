#!/usr/bin/env python
# coding:utf-8
import configparser
import os
from ftplib import FTP
from urllib import parse
import datetime
import unlzw
import gzip
import extractDCBFromSNX
import shutil

GPST0 = datetime.datetime(1980, 1, 6, 0, 0, 0)


def get_gps_weekday(date):
    seconds = (date - GPST0).total_seconds()
    week = int(seconds / 86400 / 7)
    weekday = int(seconds % (86400 * 7) / 86400)
    return week, weekday


def uncompress(file_path, dest_path='', is_delete=False):
    """Uncompress file."""
    if file_path.endswith('.gz'):
        if dest_path == '':
            dest_path = file_path.replace('.gz', '')
        g_file = gzip.GzipFile(file_path)
        uncompressed_data = g_file.read()
        g_file.close()
    elif file_path.endswith('.Z'):
        if dest_path == '':
            dest_path = file_path.replace('.Z', '')
        with open(file_path, 'rb') as f:
            compressed_data = f.read()
            uncompressed_data = unlzw.unlzw(compressed_data)
    with open(dest_path, 'w') as fw:
        fw.write(uncompressed_data.decode(encoding="utf-8"))
    if is_delete:
        os.remove(file_path)


def copy_file(srcfile, dstfile):
    if not os.path.isfile(srcfile):
        print("%s not exist!" % srcfile)
    else:
        fpath, fname = os.path.split(dstfile)  # 分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)  # 创建路径
        shutil.copyfile(srcfile, dstfile)  # 复制文件
        print("copy %s -> %s" % (srcfile, dstfile))


class DownloadFTP(object):
    def __init__(self, config_path):
        try:
            f = open(config_path)
            f.close()
        except IOError:
            print("File is not accessible.")
            exit()

        self.config_path = config_path
        self.__config = configparser.ConfigParser()
        self.__config.read(self.config_path)

    def download(self):
        """Download."""
        for prodcut in self.__config.sections():
            self.__download_product(prodcut)

    def __download_product(self, product):
        cfg = self.__config[product]
        flag = bool(int(cfg['download']))
        if flag is False:
            return
        print('%s downloading...' % product)
        ftp = cfg['ftp']
        date = datetime.datetime.strptime(cfg['date'], '%Y%m%d')
        dest = cfg['dir']

        # parse ftp
        ftp_scheme = parse.urlparse(ftp)
        host = ftp_scheme.netloc
        path = ftp_scheme.path

        # ftp session
        session = FTP(host, timeout=120)
        # session.set_debuglevel(1)
        # session.set_pasv(False)
        session.login()
        session.cwd(path)

        if product == 'sp3' or product == 'clk':
            dest = os.path.join(dest, '%d' % date.year)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            week, weekday = get_gps_weekday(date)
            product_name = 'gbm%s%s.%s.Z' % (week, weekday, product)
            session.cwd('%s' % week)
            self.__download_file(session, product_name, dest)
        elif product == 'CODG':
            dest = os.path.join(dest, '%d' % date.year)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            session.cwd('%s' % date.year)
            product_name = '%s%03d0.%02dI.Z' % (product, date.timetuple().tm_yday, date.year % 100)
            self.__download_file(session, product_name, dest)
        elif product == 'COPG':
            dest = os.path.join(dest, '%d' % date.year)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            product_name = '%s%03d0.%02dI.Z' % (product, date.timetuple().tm_yday, date.year % 100)
            self.__download_file(session, product_name, dest)
        elif product == 'COD-DCB':
            if not os.path.isdir(dest):
                os.makedirs(dest)
            session.cwd('%s' % date.year)
            product_name = 'P1C1%02d%02d.DCB' % (date.year % 100, date.month)
            self.__download_file(session, product_name, dest, is_uncompress=False)
            product_name = 'P1P2%02d%02d.DCB' % (date.year % 100, date.month)
            self.__download_file(session, product_name, dest, is_uncompress=False)
        elif product == 'CAS-DCB':
            if not os.path.isdir(dest):
                os.makedirs(dest)
            session.cwd('%s' % date.year)
            product_name = 'CAS0MGXRAP_%d%03d0000_01D_01D_DCB.BSX.gz' % (date.year, date.timetuple().tm_yday)
            self.__download_file(session, product_name, dest)
            extractDCBFromSNX.extractDCBFromSNX(os.path.join(dest, product_name).replace('.gz', ''), dest, True)
            for i in ['C1', 'P2', 'P3']:
                old_file = os.path.join(dest,'P1%s%02d%02d%02d.DCB' % (i, date.year % 100, date.month, date.day))
                new_file = os.path.join(os.path.split(dest)[0], 'P1%s%02d%02d.DCB' % (i, date.year % 100, date.month))
                copy_file(old_file, new_file)
        elif product == 'brdm':
            dest = os.path.join(dest, '%d' % date.year)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            session.cwd('%s/brdm' % date.year)
            product_name = '%s%03d0.%02dp.Z' % (product, date.timetuple().tm_yday, date.year % 100)
            self.__download_file(session, product_name, dest)
        session.quit()
        print('success')

    @staticmethod
    def __download_file(session, product_name, dest, is_uncompress=True, is_delete=True):
        """Download file."""
        file_path = os.path.join(dest, product_name)
        if product_name not in session.nlst():
            return
        session.retrbinary('RETR %s' % product_name, open(file_path, 'wb').write)
        if is_uncompress:
            uncompress(file_path, is_delete=is_delete)


if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), 'configure.ini')
    fin = open(config_path, 'r')
    file_lines = fin.readlines()
    fin.close()
    for i in range(0, 7):
        if i == 3:
            date = datetime.datetime.utcnow()
        else:
            date = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        file_lines[i * 6 + 3] = 'date = ' + date.strftime('%Y%m%d') + '\n'
    fou = open(config_path, 'w')
    fou.writelines(file_lines)
    fou.close()

    downloader = DownloadFTP(config_path)
    downloader.download()
