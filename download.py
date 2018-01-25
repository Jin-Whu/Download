#!/usr/bin/env python
# coding:utf-8


import os
from ftplib import FTP
import urlparse
import datetime
import configparser
import unlzw

GPST0 = datetime.datetime(1980, 1, 6, 0, 0, 0)


class Download(object):
    def __init__(self):
        self.config = configparser.ConfigParser()

    def init(self):
        """Init download.

        Read configure.ini and initialize donwloader.

        Returns:
            flag: True or False.
        """
        cfgpath = os.path.join(os.path.dirname(__file__), 'configure.ini')
        if not os.path.join(cfgpath):
            return False
        self.config.read(cfgpath)
        return True

    def download(self):
        """Download."""
        for prodcut in self.config.sections():
            self.__downloadproduct(prodcut)

    def __downloadproduct(self, product):
        cfg = self.config[product]
        ftp = cfg['ftp']
        sp3 = cfg['sp3']
        clk = cfg['clk']
        days = int(cfg['days'])
        dest = cfg['dest']

        # parse ftp
        ftpscheme = urlparse.urlparse(ftp)
        host = ftpscheme.netloc
        path = ftpscheme.path

        # ftp session
        session = FTP(host)
        session.login()
        session.cwd(path)

        # gps week and weekday
        date = datetime.datetime.utcnow() - datetime.timedelta(days=days)
        seconds = (date - GPST0).total_seconds()
        week = int(seconds / 86400 / 7)
        weekday = int(seconds % (86400 * 7) / 86400)
        session.cwd('%d' % week)

        if sp3 == 'yes':
            sp3product = '%s%s%s.sp3.Z' % (product, week, weekday)
            self.__download(session, sp3product, dest)
        if clk == 'yes':
            clkproduct = '%s%s%s.clk.Z' % (product, week, weekday)
            self.__download(session, clkproduct, dest)
        session.quit()

    def __download(self, session, filename, dest):
        """Download file."""
        filepath = os.path.join(dest, filename)
        session.retrbinary('RETR %s' % filename, open(filepath, 'wb').write)
        if filename.endswith('.Z'):
            destpath = filepath.replace('.Z', '')
            self.__uncompress(filepath, destpath)

    def __uncompress(self, filepath, destpath):
        """Uncompress Z file."""
        with open(filepath, 'rb') as f:
            compressed_data = f.read()
            uncompressed_data = unlzw.unlzw(compressed_data)
            with open(destpath, 'w') as fw:
                fw.write(uncompressed_data)


def process():
    downloader = Download()
    if not downloader.init():
        return
    downloader.download()


if __name__ == '__main__':
    process()
