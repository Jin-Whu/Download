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


class ConfigFTP(object):
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

    def read_config_part(self, product):
        self.product = product
        cfg = self._config[product]
        self.flag = bool(int(cfg['download']))
        if self.flag is False:
            return False
        self.mode = cfg['mode']
        if self.mode == 'auto':
            self.delay = int(cfg['delay'])
            self.date = datetime.datetime.utcnow() - datetime.timedelta(days=self.delay)
        elif self.mode == 'hand':
            self.date = datetime.datetime.strptime(cfg['date'], '%Y%m%d')
        self.ftp = cfg['ftp']
        self.dest = cfg['dir']
        return True


class DownloadFTP(object):
    def __init__(self, config_path):
        self.config = ConfigFTP(config_path)
        self.session = None

    def _login_ftp(self):
        # parse ftp
        ftp_scheme = parse.urlparse(self.config.ftp)
        host = ftp_scheme.netloc
        path = ftp_scheme.path

        # ftp session
        self.session = FTP(host, timeout=120)
        # self.session.set_debuglevel(1)
        # session.set_pasv(False)
        self.session.login()
        self.session.cwd(path)

    def _quit_ftp(self):
        if self.session is None:
            return
        self.session.quit()

    def download(self):
        """Download."""
        for prodcut in self.config._config.sections():
            if self.config.read_config_part(prodcut):
                print('%s downloading......' % self.config.product)
                self._login_ftp()
                self._download_product()
                self._quit_ftp()
                print('success')

    def _download_product(self):
        if self.config.product == 'sp3' or self.config.product == 'clk':
            self.config.dest = os.path.join(self.config.dest, '%d' % self.config.date.year)
            if not os.path.isdir(self.config.dest):
                os.makedirs(self.config.dest)
            week, weekday = get_gps_weekday(self.config.date)
            product_name = 'gbm%s%s.%s.Z' % (week, weekday, self.config.product)
            a = 1
            self.session.cwd('%d' % week)
            a = 1
            self._download_file(self.session, product_name, self.config.dest)
        elif self.config.product == 'CODG':
            dest = os.path.join(self.config.dest, '%d' % self.config.date.year)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            self.session.cwd('%d' % self.config.date.year)
            product_name = '%s%03d0.%02dI.Z' % (
                self.config.product, self.config.date.timetuple().tm_yday, self.config.date.year % 100)
            self._download_file(self.session, product_name, dest)
        elif self.config.product == 'COPG':
            self.config.dest = os.path.join(self.config.dest, '%d' % self.config.date.year)
            if not os.path.isdir(self.config.dest):
                os.makedirs(self.config.dest)
            product_name = '%s%03d0.%02dI.Z' % (
                self.config.product, self.config.date.timetuple().tm_yday, self.config.date.year % 100)
            self._download_file(self.session, product_name, self.config.dest)
        elif self.config.product == 'COD-DCB':
            if not os.path.isdir(self.config.dest):
                os.makedirs(self.config.dest)
            self.session.cwd('%d' % self.config.date.year)
            product_name = 'P1C1%02d%02d.DCB' % (self.config.date.year % 100, self.config.date.month)
            self._download_file(self.session, product_name, self.config.dest, is_uncompress=False)
            product_name = 'P1P2%02d%02d.DCB' % (self.config.date.year % 100, self.config.date.month)
            self._download_file(self.session, product_name, self.config.dest, is_uncompress=False)
        elif self.config.product == 'CAS-DCB':
            if not os.path.isdir(self.config.dest):
                os.makedirs(self.config.dest)
            self.session.cwd('%d' % self.config.date.year)
            product_name = 'CAS0MGXRAP_%d%03d0000_01D_01D_DCB.BSX.gz' % (
                self.config.date.year, self.config.date.timetuple().tm_yday)
            self._download_file(self.session, product_name, self.config.dest)
            extractDCBFromSNX.extractDCBFromSNX(os.path.join(self.config.dest, product_name).replace('.gz', ''),
                                                self.config.dest, True)
            for i in ['C1', 'P2', 'P3']:
                old_file = os.path.join(self.config.dest, 'P1%s%02d%02d%02d.DCB' % (
                    i, self.config.date.year % 100, self.config.date.month, self.config.date.day))
                new_file = os.path.join(os.path.split(self.config.dest)[0],
                                        'P1%s%02d%02d.DCB' % (i, self.config.date.year % 100, self.config.date.month))
                copy_file(old_file, new_file)
        elif self.config.product == 'brdm':
            self.config.dest = os.path.join(self.config.dest, '%d' % self.config.date.year)
            if not os.path.isdir(self.config.dest):
                os.makedirs(self.config.dest)
            self.session.cwd('%d/brdm' % self.config.date.year)
            product_name = '%s%03d0.%02dp.Z' % (
                self.config.product, self.config.date.timetuple().tm_yday, self.config.date.year % 100)
            self._download_file(self.session, product_name, self.config.dest)

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
    downloader = DownloadFTP(config_path)
    downloader.download()
