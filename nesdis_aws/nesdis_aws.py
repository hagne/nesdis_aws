# -*- coding: utf-8 -*-
import pathlib as _pl
import pandas as _pd
import s3fs as _s3fs
# import urllib as _urllib
# import html2text as _html2text
import psutil as _psutil
import numpy as _np

def readme():
    url = 'https://docs.opendata.aws/noaa-goes16/cics-readme.html'
    # html = _urllib.request.urlopen(url).read().decode("utf-8") 
    # out = _html2text.html2text(html)
    # print(out)
    print(f'follow link for readme: {url}')


def available_products():
    aws = _s3fs.S3FileSystem(anon=True)

    df = _pd.DataFrame()
    for satellite in [16,17]:
        # satellite = 16#16 (east) or 17(west)
        base_folder = _pl.Path(f'noaa-goes{satellite}')
        products_available = aws.glob(base_folder.joinpath('*').as_posix())
        df[satellite] = [p.split('/')[-1] for p in products_available if '.pdf' not in p]

    if _np.all(df[16] == df[17]):
        ins = ''
    else:
        ins = ' !!_NOT_!!'
    print(f'goes 16 and 17 products are{ins} identical')
    return df

class AwsQuery(object):
    def __init__(self,
                 path2folder_local = '/mnt/telg/tmp/aws_tmp/',
                 satellite = '16',
                 product = 'ABI-L2-AOD',
                 scan_sector = 'C',
                 start = '2020-08-08 20:00:00', 
                 end = '2020-08-09 18:00:00',
                 no_of_days = None,
                 last_x_days = None, 
                 max_no_of_files = 100,#10*24*7,
                ):
        """
        This will initialize a search on AWS.

        Parameters
        ----------
        path2folder_local : TYPE, optional
            DESCRIPTION. The default is '/mnt/telg/tmp/aws_tmp/'.
        satellite : TYPE, optional
            DESCRIPTION. The default is '16'.
        product : str, optional
            Note this is the product name described at 
            https://docs.opendata.aws/noaa-goes16/cics-readme.html 
            but without the scan sector. The default is 'ABI-L2-AOD'.
        scan_sector : str, optional
            (C)onus, (F)ull_disk, (M)eso. The default is 'C'.
        start : TYPE, optional
            DESCRIPTION. The default is '2020-08-08 20:00:00'.
        end : TYPE, optional
            DESCRIPTION. The default is '2020-08-09 18:00:00'.
        no_of_days : TYPE, optional
            DESCRIPTION. The default is None.
        last_x_days : TYPE, optional
            DESCRIPTION. The default is None.
        max_no_of_files : TYPE, optional
            DESCRIPTION. The default is 100.
        #10*24*7 : TYPE
            DESCRIPTION.
         : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        self.satellite = satellite
        self.path2folder_aws = _pl.Path(f'noaa-goes{self.satellite}')
        
        self.scan_sector = scan_sector 
        self.product = product
        
        self.start = _pd.to_datetime(start)
        self.end =  _pd.to_datetime(end)
        
        self.path2folder_local = _pl.Path(path2folder_local)
        
        self.aws = _s3fs.S3FileSystem(anon=True)
        self.aws.clear_instance_cache() # strange things happen if the is not the only query one is doing during a session
        # properties
        self._workplan = None
        
    @property
    def product(self):
        return self._product
    
    @product.setter
    def product(self, value):
        if value[-1] == self.scan_sector:
            value = value[:-1]
        self._product = value
        return
        
    def info_on_current_query(self):
        du = self.estimate_disk_usage()
        disk_space_needed = du['disk_space_needed'] * 1e-6
        disk_space_free_after_download = du['disk_space_free_after_download']
        info = (f'no of files: {self.workplan.shape[0]}\n'
                f'estimated disk usage: {disk_space_needed:0.0f} mb\n'
                f'remaining disk space after download: {disk_space_free_after_download:0.0f} %\n')
        return info
    
    # def print_readme(self):
    #     url = 'https://docs.opendata.aws/noaa-goes16/cics-readme.html'
    #     html = _urllib.request.urlopen(url).read().decode("utf-8") 
    #     out = _html2text.html2text(html)
    #     print(out)
    
    def estimate_disk_usage(self, sample_size = 10): #mega bites
        step_size = int(self.workplan.shape[0]/sample_size)
        if step_size < 1:
            step_size = 1
        sizes = self.workplan.iloc[::step_size].apply(lambda row: self.aws.disk_usage(row.path2file_aws), axis = 1)
        # sizes = self.workplan.iloc[::int(self.workplan.shape[0]/sample_size)].apply(lambda row: self.aws.disk_usage(row.path2file_aws), axis = 1)
        disk_space_needed = sizes.mean() * self.workplan.shape[0]
        
        # get remaining disk space after download
        du = _psutil.disk_usage(self.path2folder_local)
        disk_space_free_after_download = 100* (du.used + disk_space_needed)/du.total 
        out = {}
        out['disk_space_needed'] = disk_space_needed
        out['disk_space_free_after_download'] = disk_space_free_after_download
        return out
        
    @property
    def workplan(self):
        if isinstance(self._workplan, type(None)):
            # get the julian days (thus folders on aws) needed
            start_julian = int(_pd.to_datetime(self.start.date()).to_julian_date() - _pd.to_datetime(f'{self.start.year:04d}-01-01').to_julian_date()) + 1 
            end_julian = int(_pd.to_datetime(self.end.date()).to_julian_date() - _pd.to_datetime(f'{self.end.year:04d}-01-01').to_julian_date()) + 1 
            days = list(range(start_julian, end_julian+1))

            # get all the files available
#             base_folder = pl.Path(f'noaa-goes{self.satellite}')
            base_folder = self.path2folder_aws
            product_folder = base_folder.joinpath(f'{self.product}{self.scan_sector}')
            year_folder = product_folder.joinpath(f'{self.start.year}')
            files_available = []
            for day in days:
                day_folder = year_folder.joinpath(f'{day:03d}')
                hours_available = self.aws.glob(day_folder.joinpath('*').as_posix())
                hours_available = [h.split('/')[-1] for h in hours_available]

                for hour in hours_available:
                    hour_folder = day_folder.joinpath(f'{hour}')
                    glob_this = hour_folder.joinpath('*').as_posix()
                    last_glob = self.aws.glob(glob_this)
                    files_available += last_glob

            # Make workplan

            workplan = _pd.DataFrame([_pl.Path(f) for f in files_available], columns=['path2file_aws'])
            workplan['path2file_local'] = workplan.apply(lambda row: self.path2folder_local.joinpath(row.path2file_aws.name), axis = 1)

            # remove if local file exists
            workplan = workplan[~workplan.apply(lambda row: row.path2file_local.is_file(), axis = 1)]
            
            # get file sizes
#             workplan['file_size_mb'] = workplan.apply(lambda row: self.aws.disk_usage(row.path2file_aws)/1e6, axis = 1)
            
            # get the timestamp
            def row2timestamp(row):
                sos = row.path2file_aws.name.split('_')[-3]
                assert(sos[0] == 's'), f'Something needs fixing, this string ({sos}) should start with s.'
                ts = _pd.to_datetime(sos[1:-1],format = '%Y%j%H%M%S')
                return ts

            workplan.index = workplan.apply(lambda row: row2timestamp(row), axis = 1)

            # truncate ... remember so far we did not consider times in start and end, only the entire days
            workplan = workplan.sort_index()
            workplan = workplan.truncate(self.start, self.end)
            
            self._workplan = workplan
        return self._workplan       
    
    
    @workplan.setter
    def workplan(self, new_workplan):
        self._workplan = new_workplan
    
    @property
    def product_available_since(self):
        product_folder = self.path2folder_aws.joinpath(f'{self.product}{self.scan_sector}')
        years = self.aws.glob(product_folder.joinpath('*').as_posix())
        years.sort()
        
        is2000 = True
        while is2000:
            yearfolder = years.pop(0)
            firstyear = yearfolder.split('/')[-1]
            # print(firstyear)
            if firstyear != '2000':
                is2000 = False
                
        yearfolder = _pl.Path(yearfolder)
        days = self.aws.glob(yearfolder.joinpath('*').as_posix())
        days.sort()
        firstday = int(days[0].split('/')[-1])
        firstday_ts = _pd.to_datetime(firstyear) + _pd.to_timedelta(firstday, "D")
        return firstday_ts
        
    def download(self, test = False, overwrite = False, error_if_low_disk_space = True):
        if error_if_low_disk_space:
            disk_space_free_after_download = self.estimate_disk_usage()['disk_space_free_after_download']
            assert(disk_space_free_after_download<90), f"This download will bring the disk usage above 90% ({disk_space_free_after_download:0.0f}%). Turn off this error by setting error_if_low_disk_space to False."
        
        for idx, row in self.workplan.iterrows():
            if not overwrite:
                if row.path2file_local.is_file():
                    continue
                
            out = self.aws.get(row.path2file_aws.as_posix(), row.path2file_local.as_posix())
            if test:
                break
        return out
        