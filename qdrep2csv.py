#!/usr/bin/env python
# encoding: utf-8

import argparse
import os
import sqlite3
from tqdm import tqdm,trange
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

def save_to_csv(list_to_pandas, csv_filename):
    df = pd.DataFrame(list_to_pandas, columns =['nvtx_text', 'nvtx_start', 'nvtx_end', 'device', 'crt_name', 'crt_start', 'crt_end', 'kernel_mangled_name', 'kernel_stream', 'kernel_start', 'kernel_end', 'kernel_duration'])
    if not os.path.isfile(csv_filename):
        df.to_csv(csv_filename, index=False)
    else:
        df.to_csv(csv_filename, index=False, mode='a', header=False)
    list_to_pandas.clear()

def export_single_step(conn, csv_filename, item):
    list_to_pandas = []
    nvtx_start = item[0]
    nvtx_end = item[1]
    globalTid = item[2]
    items = query_sqlite(conn, 'SELECT CUPTI_ACTIVITY_KIND_RUNTIME.nameId, CUPTI_ACTIVITY_KIND_KERNEL.demangledName, CUPTI_ACTIVITY_KIND_KERNEL.deviceId, CUPTI_ACTIVITY_KIND_KERNEL.streamId, CUPTI_ACTIVITY_KIND_KERNEL.start, CUPTI_ACTIVITY_KIND_KERNEL.end, CUPTI_ACTIVITY_KIND_RUNTIME.start, CUPTI_ACTIVITY_KIND_RUNTIME.end FROM CUPTI_ACTIVITY_KIND_RUNTIME JOIN CUPTI_ACTIVITY_KIND_KERNEL ON CUPTI_ACTIVITY_KIND_RUNTIME.correlationId=CUPTI_ACTIVITY_KIND_KERNEL.correlationId where CUPTI_ACTIVITY_KIND_RUNTIME.globalTid="{0}" and (CUPTI_ACTIVITY_KIND_RUNTIME.start>={1} and CUPTI_ACTIVITY_KIND_RUNTIME.start<={2} or CUPTI_ACTIVITY_KIND_RUNTIME.end>={1} and CUPTI_ACTIVITY_KIND_RUNTIME.end<={2}) and (CUPTI_ACTIVITY_KIND_KERNEL.start>={1} and CUPTI_ACTIVITY_KIND_KERNEL.start<={2} or CUPTI_ACTIVITY_KIND_KERNEL.end>={1} and CUPTI_ACTIVITY_KIND_KERNEL.end<={2});'.format(globalTid, nvtx_start, nvtx_end))
    for i in trange(len(items)):
        item = items[i]
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
            item1s = query_sqlite(conn, 'SELECT start,end,text FROM NVTX_EVENTS where globalTid="{0}" and start<={1} and end>={2} limit 3;'.format(globalTid, max(crt_start, knl_start), min(crt_end, knl_end)))
            nvtx_start = 0
            nvtx_end = 0
            nvtx_text = ''
            if len(item1s) > 2:
                nvtx_start = item1s[2][0]
                nvtx_end = item1s[2][1]
                nvtx_text = item1s[2][2]
            list_to_pandas.append([nvtx_text, nvtx_start, nvtx_end, device, crt_name, crt_start, crt_end, knl_name, knl_stream, knl_start, knl_end, knl_dur])
            if len(list_to_pandas) == 1000:
                save_to_csv(list_to_pandas, csv_filename)
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
    if len(list_to_pandas) > 0:
        save_to_csv(list_to_pandas, csv_filename)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('sqlite_file')
    args = parser.parse_args()
    if not os.path.isfile(args.sqlite_file):
        print('{} doesn\'t exist.'.format(args.sqlite_file))
        exit(1)
    csv_filename = args.sqlite_file.replace('.sqlite', '.csv')
    print('Data will be saved to {}'.format(csv_filename))
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
        gTids = query_sqlite(conn, 'SELECT distinct(globalTid) from NVTX_EVENTS where text="train_step";')
        counts_gTid = []
        print('Number of GPUs: {}'.format(len(gTids)))
        for i in range(len(gTids)):
            count = query_sqlite(conn, 'SELECT count(*) from NVTX_EVENTS where text="train_step" and globalTid="{}";'.format(gTids[i][0]))
            counts_gTid.append(count[0][0])
            print('  Number of steps on GPU{}: {}'.format(i, count[0][0]))
        while True:
            iGPU = input('Please select which GPU you would like the steps to be exported. 0 for all GPUS: ')
            if not iGPU.isdigit() or int(iGPU) > len(gTids):
                print('Please input a digit in range [0, {}]'.format(len(gTids)))
            else:
                break
        iGPU = int(iGPU)
        while True:
            iStep = input('Please select which step you would like to export. 0 for all steps: ')
            if not iStep.isdigit() or int(iStep) > counts_gTid[iGPU-1]:
                print('Please input a digit in range [0, {}]'.format(counts_gTid[iGPU-1]))
            else:
                break
        iStep = int(iStep)
        if iGPU > 0:
            sql = 'SELECT start,end,globalTid from NVTX_EVENTS where text="train_step" and globalTid="{}"'.format(gTids[iGPU-1][0])
            items = query_sqlite(conn, sql)
            if iStep > 0:
                export_single_step(conn, csv_filename, items[iStep-1])
            else:
                for i in trange(len(items)):
                    export_single_step(conn, csv_filename, items[i])
        else:
            for i in trange(len(gTids)):
                sql = 'SELECT start,end,globalTid from NVTX_EVENTS where text="train_step" and globalTid="{}"'.format(gTids[i][0])
                items = query_sqlite(conn, sql)
                if iStep > 0:
                    export_single_step(conn, csv_filename, items[iStep-1])
                else:
                    for i in trange(len(items)):
                        export_single_step(conn, csv_filename, items[i])
    close_sqlite(conn)

if __name__ == '__main__':
    main()
