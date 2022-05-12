# -*- coding: utf-8 -*-
import pathlib as _pl
import pandas as _pd
import s3fs as _s3fs
# import urllib as _urllib
# import html2text as _html2text
import psutil as _psutil
import numpy as _np
# import xarray as _xr
import warnings
from functools import partial
import multiprocessing as mp

def readme():
    url = 'https://docs.opendata.aws/noaa-goes16/cics-readme.html'
    # html = _urllib.request.urlopen(url).read().decode("utf-8") 
    # out = _html2text.html2text(html)
    # print(out)
    print(f'follow link for readme: {url}')

variable_info = {'ABI-L1b-Rad': 'Radiances',
                 'ABI-L2-ACHA': 'Cloud Top Height',
                 'ABI-L2-ACHT': 'Cloud Top Temperature',
                 'ABI-L2-ACM': 'Clear Sky Mask',
                 'ABI-L2-ACTP': 'Cloud Top Phase',
                 'ABI-L2-ADP': 'Aerosol Detection',
                 'ABI-L2-AICE': 'Ice Concentration and Extent',
                 'ABI-L2-AITA': 'Ice Age and Thickness',
                 'ABI-L2-AOD': 'Aerosol Optical Depth',
                 'ABI-L2-BRF': 'Land Surface Bidirectional Reflectance Factor () 2 km resolution & DQFs',
                 'ABI-L2-CMIP': 'Cloud and Moisture Imagery',
                 'ABI-L2-COD': 'Cloud Optical Depth',
                 'ABI-L2-CPS': 'Cloud Particle Size',
                 'ABI-L2-CTP': 'Cloud Top Pressure',
                 'ABI-L2-DMW': 'Derived Motion Winds',
                 'ABI-L2-DMWV': 'L2+ Derived Motion Winds',
                 'ABI-L2-DSI': 'Derived Stability Indices',
                 'ABI-L2-DSR': 'Downward Shortwave Radiation',
                 'ABI-L2-FDC': 'Fire (Hot Spot Characterization)',
                 'ABI-L2-LSA': 'Land Surface Albedo () 2km resolution & DQFs',
                 'ABI-L2-LST': 'Land Surface Temperature',
                 'ABI-L2-LST2KM': 'Land Surface Temperature',
                 'ABI-L2-LVMP': 'Legacy Vertical Moisture Profile',
                 'ABI-L2-LVTP': 'Legacy Vertical Temperature Profile',
                 'ABI-L2-MCMIP': 'Cloud and Moisture Imagery',
                 'ABI-L2-RRQPE': 'Rainfall Rate (Quantitative Precipitation Estimate)',
                 'ABI-L2-RSR': 'Reflected Shortwave Radiation Top-Of-Atmosphere',
                 'ABI-L2-SST': 'Sea Surface (Skin) Temperature',
                 'ABI-L2-TPW': 'Total Precipitable Water',
                 'ABI-L2-VAA': 'Volcanic Ash: Detection and Height'}

def get_available_products():
    aws = _s3fs.S3FileSystem(anon=True)

    products = []
    for satellite in [16,17]:
        # satellite = 16#16 (east) or 17(west)
        base_folder = _pl.Path(f'noaa-goes{satellite}')
        products_available = aws.glob(base_folder.joinpath('*').as_posix())
        # df[satellite] = 
        products += [p.split('/')[-1] for p in products_available if '.pdf' not in p]

    products = _np.unique(products)

    products = _np.unique([p[:-1] for p in products])

    products = [p for p in products if p[:3] =='ABI']

    product_avail = _pd.DataFrame(columns = ['16-C', '16-F', '16-M', '17-C', '17-F', '17-M'], index = products)

    def get_first_day(product, satellite, scan_sector):
        prod = product
        product_folder = _pl.Path(f'noaa-goes{satellite}/{prod}{scan_sector}')

        years = aws.glob(product_folder.joinpath('*').as_posix())
        if len(years) == 0:
            return False

        years.sort()

        is2000 = True
        while is2000:
            yearfolder = years.pop(0)
            firstyear = yearfolder.split('/')[-1]
            # print(firstyear)
            if firstyear != '2000':
                is2000 = False

        yearfolder = _pl.Path(yearfolder)
        days = aws.glob(yearfolder.joinpath('*').as_posix())
        days.sort()
        firstday = int(days[0].split('/')[-1])
        fd = _pd.to_datetime(firstyear) + _pd.to_timedelta(firstday, "D")
        return fd

    for prod, row in product_avail.iterrows():
        # break

        for idx,val in row.iteritems():
            # break
            satellite, scan_sector = idx.split('-')
            fd = get_first_day(prod, satellite, scan_sector)
            if fd:
                product_avail.loc[prod, idx] = f'{fd.year:04d}-{fd.month:02d}-{fd.day:02d}'
            else:
                product_avail.loc[prod, idx] = '-'
                
    product_avail.insert(0,'longname', product_avail.apply(lambda row: variable_info[row.name], axis=1))
    product_avail.index.name = 'product'
    return product_avail



class AwsQuery(object):
    def __init__(self,
                 path2folder_local = '/mnt/telg/tmp/aws_tmp/',
                 satellite = '16',
                 product = 'ABI-L2-AOD',
                 scan_sector = 'C',
                 start = '2020-08-08 20:00:00', 
                 end = '2020-08-09 18:00:00',
                 process = None,
                 keep_files = None,
                 verbose = False,
                 # check_if_file_exist = True,
                 # no_of_days = None,
                 # last_x_days = None, 
                 # max_no_of_files = 100,#10*24*7,
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
        process: dict,
            This is still in development and might be buggy.
            Example:
                dict(concatenate = 'daily',
                     function = lambda row: some_function(row, *args, **kwargs),
                     prefix = 'ABI_L2_AOD_processed',
                     path2processed = '/path2processed/')
        keep_files: bool, optional
            Default is True unless process is given which changes the default
            False.

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
        
        if isinstance(process, dict):
            self._process = True
            # self._process_concatenate = process['concatenate']
            self._process_function = process['function']
            self._process_name_prefix = process['prefix']
            self._process_path2processed = _pl.Path(process['path2processed'])
            # self._process_path2processed_tmp = self._process_path2processed.joinpath('tmp')
            # self._process_path2processed_tmp.mkdir(exist_ok=True)
            if keep_files:
                self.keep_files = True
            else:
                self.keep_files = False
            # self.check_if_file_exist = False
        else:
            self._process = False
        
        #### TODO memoryleak initiate aws only when its needed
        self.aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True)
        # self.aws.clear_instance_cache() # strange things happen if the is not the only query one is doing during a session
        # properties
        self._workplan = None
        self._verbose = verbose
        
    @property
    def product(self):
        return self._product
    
    @product.setter
    def product(self, value):
        if value[-1] == self.scan_sector:
            warnings.warn('last letter of product is equal to scan sector ... by mistake?')
        self._product = value
        return
        
    def info_on_current_query(self):
        nooffiles = self.workplan.shape[0]
        if nooffiles == 0:
            info = 'no file found or all files already on disk.'
        else:
            du = self.estimate_disk_usage()
            disk_space_needed = du['disk_space_needed'] * 1e-6
            disk_space_free_after_download = du['disk_space_free_after_download']
            info = (f'no of files: {nooffiles}\n'
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
        #### TODO memory leak: reload instance
        # self.aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True)
        sizes = self.workplan.iloc[::step_size].apply(lambda row: self.aws.disk_usage(row.path2file_aws), axis = 1)
        # sizes = self.workplan.iloc[::int(self.workplan.shape[0]/sample_size)].apply(lambda row: self.aws.disk_usage(row.path2file_aws), axis = 1)
        disk_space_needed = sizes.mean() * self.workplan.shape[0]
        
        # get remaining disk space after download
        du = _psutil.disk_usage(self.path2folder_local)
        disk_space_free_after_download = 100 - (100* (du.used + disk_space_needed)/du.total )
        out = {}
        out['disk_space_needed'] = disk_space_needed
        out['disk_space_free_after_download'] = disk_space_free_after_download
        return out
        
    @property
    def workplan(self):
        if isinstance(self._workplan, type(None)):
            if self._verbose:
                print('Get workplan:')
#             #### bug: problem below is that time ranges that span over multiple years will not work!
#             # get the julian days (thus folders on aws) needed
#             start_julian = int(_pd.to_datetime(self.start.date()).to_julian_date() - _pd.to_datetime(f'{self.start.year:04d}-01-01').to_julian_date()) + 1 
#             end_julian = int(_pd.to_datetime(self.end.date()).to_julian_date() - _pd.to_datetime(f'{self.end.year:04d}-01-01').to_julian_date()) + 1 
#             days = list(range(start_julian, end_julian+1))

#             # get all the files available
# #             base_folder = pl.Path(f'noaa-goes{self.satellite}')
#             base_folder = self.path2folder_aws
#             product_folder = base_folder.joinpath(f'{self.product}{self.scan_sector}')
#             files_available = []
#             year_folder = product_folder.joinpath(f'{self.start.year}')
#             for day in days:
#                 day_folder = year_folder.joinpath(f'{day:03d}')
#                 hours_available = self.aws.glob(day_folder.joinpath('*').as_posix())
#                 hours_available = [h.split('/')[-1] for h in hours_available]

#                 for hour in hours_available:
#                     hour_folder = day_folder.joinpath(f'{hour}')
#                     glob_this = hour_folder.joinpath('*').as_posix()
#                     last_glob = self.aws.glob(glob_this)
#                     files_available += last_glob
            
            #### make a data frame to all the available files in the time range
            # create a dataframe with all hours in the time range
            df = _pd.DataFrame(index = _pd.date_range(self.start, self.end, freq='h'), columns=['path'])
            
            # create the path to the directory of each row above (one per houre)
            product_folder = self.path2folder_aws.joinpath(f'{self.product}{self.scan_sector}')
            df['path'] = df.apply(lambda row: product_folder.joinpath(str(row.name.year)).joinpath(f'{row.name.day_of_year:03d}').joinpath(f'{row.name.hour:02d}').joinpath('*'), axis= 1)
            
            # get the path to each file in all the folders 
            files_available = []
            
            #### TODO memory leak: below reload aws instance            
            # self.aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True)
            for idx,row in df.iterrows():
                files_available += self.aws.glob(row.path.as_posix())

            #### Make workplan

            workplan = _pd.DataFrame([_pl.Path(f) for f in files_available], columns=['path2file_aws'])
            workplan['path2file_local'] = workplan.apply(lambda row: self.path2folder_local.joinpath(row.path2file_aws.name), axis = 1)

            #### remove if local file exists
            if not self._process:
                workplan = workplan[~workplan.apply(lambda row: row.path2file_local.is_file(), axis = 1)]
            
            # get file sizes ... takes to long to do for each file
#             workplan['file_size_mb'] = workplan.apply(lambda row: self.aws.disk_usage(row.path2file_aws)/1e6, axis = 1)
            
            #### get the timestamp
            def row2timestamp(row):
                sos = row.path2file_aws.name.split('_')[-3]
                assert(sos[0] == 's'), f'Something needs fixing, this string ({sos}) should start with s.'
                ts = _pd.to_datetime(sos[1:-1],format = '%Y%j%H%M%S')
                return ts

            workplan.index = workplan.apply(lambda row: row2timestamp(row), axis = 1)

            #### truncate ... remember so far we did not consider times in start and end, only the entire days
            workplan = workplan.sort_index()
            workplan = workplan.truncate(self.start, self.end)
            
            #### processing additions
            if self._process:
                ### add path to processed file names
                workplan["path2file_local_processed"] = workplan.apply(lambda row: self._process_path2processed.joinpath(f'{self._process_name_prefix}_{row.name.year}{row.name.month:02d}{row.name.day:02d}_{row.name.hour:02d}{row.name.minute:02d}{row.name.second:02d}.nc'), axis = 1)
                ### remove if file exists 
                workplan = workplan[~workplan.apply(lambda row: row.path2file_local_processed.is_file(), axis = True)]
                # workplan['path2file_tmp'] = workplan.apply(lambda row: self._process_path2processed_tmp.joinpath(row.name.__str__()), axis = 1)
                
            
            self._workplan = workplan
            if self._verbose:
                print('workplan done')
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
        
    def download(self, test = False, overwrite = False, alternative_workplan = False,
                 error_if_low_disk_space = True):
        """
        

        Parameters
        ----------
        test : TYPE, optional
            DESCRIPTION. The default is False.
        overwrite : TYPE, optional
            DESCRIPTION. The default is False.
        alternative_workplan : pandas.Dataframe, optional
            This will ignore the instance workplan and use the provided one 
            instead. The default is False.
        error_if_low_disk_space : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        out : TYPE
            DESCRIPTION.

        """
        if isinstance(alternative_workplan, _pd.DataFrame):
            workplan = alternative_workplan
        else:
            workplan = self.workplan
        
        if error_if_low_disk_space:
            disk_space_free_after_download = self.estimate_disk_usage()['disk_space_free_after_download']
            assert(disk_space_free_after_download<90), f"This download will bring the disk usage above 90% ({disk_space_free_after_download:0.0f}%). Turn off this error by setting error_if_low_disk_space to False."
        
        for idx, row in workplan.iterrows():
            if not overwrite:
                if row.path2file_local.is_file():
                    continue
            #### TODO memory leak ... this did not help ... the following line is trying to deal with it. Its actaully not clear if the leak only happens when processing ... then the following line should not help
            # self.aws.clear_instance_cache() 
            # next try: reload aws instance: not helping
            # self.aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True)
            out = self.aws.get(row.path2file_aws.as_posix(), row.path2file_local.as_posix())
            if test:
                break
        return out
    
    
    def process(self, raise_exception = False, verbose = False):
    # deprecated first grouping is required
        # group = self.workplan.groupby('path2file_local_processed')
        # for p2flp, p2flpgrp in group:
        #     break
        ## for each file in group
        if verbose:
            print(f'start processing ({self.workplan.shape[0]}): ', end = '')
        for dt, row in self.workplan.iterrows():
            if verbose:
                print('.', end = '')
            if row.path2file_local_processed.is_file():
                continue
            if not row.path2file_local.is_file():
    #             print('downloading')
                #### download
                # download_output = 
                
                #### TODO memory leak ... i did not notice that the download is done separately here... maybe try out the cach purch only
                # self.aws.clear_instance_cache()     #-> not helping           
                # self.aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True) - not helping
                self.aws.get(row.path2file_aws.as_posix(), row.path2file_local.as_posix())
            #### process
            try:
                #### TODO memory leak check if row is the same before and after
                rowold = row.copy()
                self._process_function(row)
                if not row.equals(rowold):
                    print('row changed ... return')
                    return row, rowold
                if verbose:
                    print(':', end = '')
            except:
                if raise_exception:
                    raise
                else:
                    print(f'error applying function on one file {row.path2file_local.name}. The raw fill will still be removed (unless keep_files is True) to avoid storage issues')
            #### remove raw file
            if not self.keep_files:
                row.path2file_local.unlink()
            if verbose:
                print('|', end = '')
        if verbose:
            print('Done')      
        return
    
    #### TODO now since I am using multiprossing.Process instead of Pool we might want to separate the nesdis packages again.
    def process_parallel(self, process_function = None, args = {}, no_of_cpu = 2, raise_exception = False, path2log= None, subprocess = '',server = '', comment = '', verbose = False):
    # deprecated first grouping is required
        # group = self.workplan.groupby('path2file_local_processed')
        # for p2flp, p2flpgrp in group:
        #     break
        ## for each file in group
        if verbose:
            print(f'start processing ({self.workplan.shape[0]}): ', end = '')
        # for dt, row in self.workplan.iterrows():
        
        if verbose:
            print('Done')  
        
  
        if 0:
            # pool = mp.Pool(processes=no_of_cpu)
            pool = mp.get_context('spawn').Pool(processes=no_of_cpu)
            idx, rows = zip(*list(self.workplan.iterrows()))
            pool.map(partial(process_function, **args), rows)
            pool.close()
            pool.join()
        
        else:
            
            mp.set_start_method('spawn')
            idx, rows = zip(*list(self.workplan.iterrows()))
            for row_sub in zip(*[list(l) for l in _np.array_split(rows, no_of_cpu)]):
                print('=', end = '', flush = True)
                subproslist = []
                
                # print(f'row_sub: {row_sub}', flush = True)
                for row in row_sub:
                    rowt= rows[0].copy()
                    rowt.iloc[:] = row
                    row = rowt
                    print(',',  end = '', flush = True)
                    # print(f'row: {row}', flush = True)
                    process = mp.Process(target=process_function, args = (row,), kwargs = args)
                    process.start()
                    subproslist.append(process)
                [p.join() for p in subproslist]
                
                datetime = _pd.Timestamp.now()
                run_status = 1
                error = 0
                success = no_of_cpu
                warning = 0
                if not isinstance(path2log, type(None)):
                    with open(path2log, 'a') as log_out:
                            log_out.write(f'{datetime},{run_status},{error},{success},{warning},{subprocess},{server},{comment}\n')
                
                # break
        return
        #### todo: concatenate 
        # if this is actually desired I would think this should be done seperately, not as part of this package
        # try:
        #     ds = _xr.open_mfdataset(p2flpgrp.path2file_tmp)

        #     #### save final product
        #     ds.to_netcdf(p2flp)
        
        #     #### remove all tmp files
        #     if not keep_tmp_files:
        #         for dt, row in p2flpgrp.iterrows():
        #             try:
        #                 row.path2file_tmp.unlink()
        #             except FileNotFoundError:
        #                 pass
        # except:
        #     print('something went wrong with the concatenation. The file will not be removed')
        
def test(f1):
    def f(x):
        f1(x)
        # f2(x)
    return f 

def process_row(row):#, process_function = None):
    # if verbose:
    #     print('.', end = '')
    return row #lambda x: x

    if row.path2file_local_processed.is_file():
        return
    if not row.path2file_local.is_file():
#             print('downloading')
        #### download
        # download_output = 
        
        #### TODO memory leak ... i did not notice that the download is done separately here... maybe try out the cach purch only
        # self.aws.clear_instance_cache()     #-> not helping           
        aws = _s3fs.S3FileSystem(anon=True, skip_instance_cache=True) #- not helping
        aws.get(row.path2file_aws.as_posix(), row.path2file_local.as_posix())
        aws.close()
    #### process
    raise_exception = True
    try:
        #### TODO memory leak check if row is the same before and after
        rowold = row.copy()
        process_function(row)
        if not row.equals(rowold):
            print('row changed ... return')
            return row, rowold
        # if verbose:
        #     print(':', end = '')
    except:
        if raise_exception:
            raise
        else:
            print(f'error applying function on one file {row.path2file_local.name}. The raw fill will still be removed (unless keep_files is True) to avoid storage issues')
    #### remove raw file
    # if not self.keep_files:
    row.path2file_local.unlink()
    # if verbose:
    #     print('|', end = '')