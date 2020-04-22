#!/usr/bin/env python
# encoding: utf-8

import argparse
import os
import sqlite3
from tqdm import tqdm
import pandas as pd

def init_sqlite(filename):
    if not filename.endswith('.sqlite'):
        print('Please input a sqlite file')
        return None, None
    conn = sqlite3.connect(filename)
    return conn

def close_sqlite(conn):
    conn.close()

def query_sqlite(conn, cmd):
    cursor = conn.execute(cmd)
    tables = [
        v for v in cursor.fetchall()
    ]
    cursor.close()
    return tables

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sqlite_file')
    args = parser.parse_args()
    csv_filename = args.sqlite_file.replace('.sqlite', '.csv')
    if os.path.isfile(csv_filename):
        g = input('{} exists.\nDo you want to overwrite it? [y]/n: '.format(csv_filename))
        if g == '' or g == 'Y' or g == 'y' or g == 'Yes' or g == 'yes':
            os.remove(csv_filename)
        else:
            exit(0)

    conn = init_sqlite(args.sqlite_file)
    items = query_sqlite(conn, 'SELECT * FROM sqlite_master WHERE type="table" and name="NVTX_EVENTS";')
    if len(items) == 0:
        print('NVTX_EVENTS not found.')
    else:
        list_to_pandas = []
        items = query_sqlite(conn, 'SELECT CUPTI_ACTIVITY_KIND_RUNTIME.nameId, CUPTI_ACTIVITY_KIND_KERNEL.demangledName, CUPTI_ACTIVITY_KIND_KERNEL.deviceId, CUPTI_ACTIVITY_KIND_KERNEL.streamId, CUPTI_ACTIVITY_KIND_KERNEL.start, CUPTI_ACTIVITY_KIND_KERNEL.end, CUPTI_ACTIVITY_KIND_RUNTIME.start, CUPTI_ACTIVITY_KIND_RUNTIME.end FROM CUPTI_ACTIVITY_KIND_RUNTIME inner join CUPTI_ACTIVITY_KIND_KERNEL on CUPTI_ACTIVITY_KIND_RUNTIME.correlationId=CUPTI_ACTIVITY_KIND_KERNEL.correlationId;')
        for item in tqdm(items):
            rtName = query_sqlite(conn, 'SELECT value from StringIds where id={}'.format(item[0]))
            knName = query_sqlite(conn, 'SELECT value from StringIds where id={}'.format(item[1]))
            if len(rtName) == 1 and len(knName) == 1:
                device = item[2]
                crt_name = rtName[0][0]
                crt_start = item[6]
                crt_end = item[7]
                knl_name = knName[0][0]
                knl_stream = item[3]
                knl_start = item[4]
                knl_end = item[5]
                knl_dur = knl_end - knl_start
                nvtx = query_sqlite(conn, 'SELECT start,end,text from NVTX_EVENTS where start<={0} and end>={0} or start<={1} and end>={1} limit 2;'.format(crt_start, crt_end))
                if len(nvtx) > 1:
                    nvtx_text = nvtx[1][2]
                    nvtx_start = nvtx[1][0]
                    nvtx_end = nvtx[1][1]
                    list_to_pandas.append([nvtx_text, nvtx_start, nvtx_end, device, crt_name, crt_start, crt_end, knl_name, knl_stream, knl_start, knl_end, knl_dur])
                    if len(list_to_pandas) > 1000:
                        df = pd.DataFrame(list_to_pandas, columns =['nvtx_text', 'nvtx_start', 'nvtx_end', 'device', 'crt_name', 'crt_start', 'crt_end', 'kernel_mangled_name', 'kernel_stream', 'kernel_start', 'kernel_end', 'kernel_duration'])
                        if not os.path.isfile(csv_filename):
                            df.to_csv(csv_filename)
                            # print('Data written to {}'.format(csv_filename))
                        else:
                            df.to_csv(csv_filename, mode='a', header=False)
                            # print('Data written to {}'.format(csv_filename))
                        list_to_pandas.clear()
                    # print('')
                    # print('nvtx_text: {}'.format(nvtx_text))
                    # print('nvtx_start: {}'.format(nvtx_start))
                    # print('nvtx_end: {}'.format(nvtx_end))
                    # print('device: {}'.format(device))
                    # print('crt_name: {}'.format(crt_name))
                    # print('crt_start: {}'.format(crt_start))
                    # print('crt_end: {}'.format(crt_end))
                    # print('kernel_mangled_name:{}'.format(knl_name))
                    # print('kernel_stream: {}'.format(knl_stream))
                    # print('kernel_start: {}'.format(knl_start))
                    # print('kernel_end: {}'.format(knl_end))
                    # print('kernel_duration: {}'.format(knl_dur))
    close_sqlite(conn)

if __name__ == '__main__':
    main()
