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
        # print("copy %s -> %s" % (srcfile, dstfile))


class Config(object):
    def __init__(self, config_path):
        try:
            f = open(config_path)
            f.close()
        except IOError:
            print("File is not accessible.")
            exit()

        self.__config_path = config_path
        self._config = configparser.ConfigParser()
        self._config.read(self.__config_path)
        self.current_date = GPST0

    def read_config_part(self, product):
        self.product = product
        cfg = self._config[product]
        self.flag = bool(int(cfg['download']))
        if self.flag is False:
            return False
        self.mode = cfg['mode']
        if self.mode == 'auto':
            self.delay = int(cfg['delay'])
            self.start_date = datetime.datetime.utcnow() - datetime.timedelta(days=self.delay)
            self.start_date = datetime.datetime(self.start_date.year, self.start_date.month, self.start_date.day, 0, 0, 0)
            self.end_date = self.end_date
        elif self.mode == 'hand':
            self.start_date = datetime.datetime.strptime(cfg['start_date'], '%Y%m%d')
            self.end_date = datetime.datetime.strptime(cfg['end_date'], '%Y%m%d')
        self.ftp = cfg['ftp']
        self.dest = cfg['dir']
        return True


class Session(object):
    def __init__(self):
        self.path = None
        self.host = None

    def login_ftp(self, ftp):
        # parse ftp
        ftp_scheme = parse.urlparse(ftp)
        self.host = ftp_scheme.netloc
        self.path = ftp_scheme.path

        # ftp session
        self.session = FTP(self.host, timeout=120)
        # self.session.set_debuglevel(1)
        # session.set_pasv(False)
        self.session.login()
        self.session.cwd(self.path)

    def quit_ftp(self):
        if self.session is None:
            return
        self.session.quit()


class DownloadFTP(object):
    def __init__(self, config_path):
        if config_path != '':
            self.configFTP = Config(config_path)
        self.sessionFTP = Session()

    def download(self):
        """Download."""
        for prodcut in self.configFTP._config.sections():
            if self.configFTP.read_config_part(prodcut):
                print('%s downloading......' % self.configFTP.product)
                self.sessionFTP.login_ftp(self.configFTP.ftp)
                try:
                    self._download_product()
                except FileNotFoundError:
                    print('file not found')
                else:
                    print('success')
                self.sessionFTP.quit_ftp()

    def _download_product(self):
        self.configFTP.current_date = self.configFTP.start_date
        while self.configFTP.current_date <= self.configFTP.end_date:
            if self.configFTP.product == 'sp3' or self.configFTP.product == 'clk':
                dest = os.path.join(self.configFTP.dest, '%d' % self.configFTP.current_date.year)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                week, weekday = get_gps_weekday(self.configFTP.current_date)
                product_name = 'gbm%s%s.%s.Z' % (week, weekday, self.configFTP.product)
                self.sessionFTP.session.cwd('%s/%d' % (self.sessionFTP.path, week))
                self._download_file(self.sessionFTP.session, product_name, dest)
            elif self.configFTP.product == 'CODG':
                dest = os.path.join(self.configFTP.dest, '%d' % self.configFTP.current_date.year)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                self.sessionFTP.session.cwd('%s/%d' % (self.sessionFTP.path, self.configFTP.current_date.year))
                product_name = '%s%03d0.%02dI.Z' % (
                    self.configFTP.product, self.configFTP.current_date.timetuple().tm_yday, self.configFTP.current_date.year % 100)
                self._download_file(self.sessionFTP.session, product_name, dest)
            elif self.configFTP.product == 'COPG':
                dest = os.path.join(self.configFTP.dest, '%d' % self.configFTP.current_date.year)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                product_name = '%s%03d0.%02dI.Z' % (
                    self.configFTP.product, self.configFTP.current_date.timetuple().tm_yday, self.configFTP.current_date.year % 100)
                self._download_file(self.sessionFTP.session, product_name, dest)
            elif self.configFTP.product == 'COD-DCB':
                if not os.path.isdir(self.configFTP.dest):
                    os.makedirs(self.configFTP.dest)
                self.sessionFTP.session.cwd('%s/%d' % (self.sessionFTP.path,self.configFTP.current_date.year))
                product_name = 'P1C1%02d%02d.DCB' % (self.configFTP.current_date.year % 100, self.configFTP.current_date.month)
                self._download_file(self.sessionFTP.session, product_name, self.configFTP.dest, is_uncompress=False)
                product_name = 'P1P2%02d%02d.DCB' % (self.configFTP.current_date.year % 100, self.configFTP.current_date.month)
                self._download_file(self.sessionFTP.session, product_name, self.configFTP.dest, is_uncompress=False)
            elif self.configFTP.product == 'CAS-DCB':
                if not os.path.isdir(self.configFTP.dest):
                    os.makedirs(self.configFTP.dest)
                self.sessionFTP.session.cwd('%s/%d' % (self.sessionFTP.path, self.configFTP.current_date.year))
                product_name = 'CAS0MGXRAP_%d%03d0000_01D_01D_DCB.BSX.gz' % (
                    self.configFTP.current_date.year, self.configFTP.current_date.timetuple().tm_yday)
                self._download_file(self.sessionFTP.session, product_name, self.configFTP.dest)
                extractDCBFromSNX.extractDCBFromSNX(os.path.join(self.configFTP.dest, product_name).replace('.gz', ''),
                                                    self.configFTP.dest, True)
                for i in ['C1', 'P2', 'P3']:
                    old_file = os.path.join(self.configFTP.dest, 'P1%s%02d%02d%02d.DCB' % (
                        i, self.configFTP.current_date.year % 100, self.configFTP.current_date.month, self.configFTP.current_date.day))
                    new_file = os.path.join(os.path.split(self.configFTP.dest)[0],
                                            'P1%s%02d%02d.DCB' % (i, self.configFTP.current_date.year % 100, self.configFTP.current_date.month))
                    copy_file(old_file, new_file)
            elif self.configFTP.product == 'brdm':
                dest = os.path.join(self.configFTP.dest, '%d' % self.configFTP.current_date.year)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                self.sessionFTP.session.cwd('%s/%d/brdm' % (self.sessionFTP.path, self.configFTP.current_date.year))
                product_name = '%s%03d0.%02dp.Z' % (
                    self.configFTP.product, self.configFTP.current_date.timetuple().tm_yday, self.configFTP.current_date.year % 100)
                self._download_file(self.sessionFTP.session, product_name, dest)
            self.configFTP.current_date += datetime.timedelta(days=1)

    @staticmethod
    def _download_file(session, product_name, dest, is_uncompress=True, is_delete=True):
        """Download file."""
        file_path = os.path.join(dest, product_name)
        if product_name not in session.nlst():
            return
        session.retrbinary('RETR %s' % product_name, open(file_path, 'wb').write)
        if is_uncompress:
            uncompress(file_path, is_delete=is_delete)


if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), 'configure.ini')
    downloaderFTP = DownloadFTP(config_path)
    downloaderFTP.download()
