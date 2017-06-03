import argparse
import csv
import logging
import os
import shutil
import subprocess
import sys
import time

__TPCH_DBGEN_PATH__ = './tpch-dbgen'
__OUTPUT_PATH__ = './data'
__NAME_LIST__ = ['customer', 'part', 'partsupp',
                 'region', 'supplier', 'nation', 'orders', 'lineitem']
__SCALE_FACTOR__ = '1'
__UPDATE_FACTOR__ = '1'
__JAVA_CMD__ = ['java', '-Dfile.encoding=UTF-8', '-classpath', '/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/bin:/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/build/gradle-ivyxml-plugin-0.3.2.jar:/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/build/gradle-javaPropFile-plugin-0.6.0.jar:/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/build/gradle/wrapper/gradle-wrapper.jar:/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/lib/hsqldb.jar:/Users/Cyclops-THSS/Documents/Eclipse/hsqldb/lib/sqltool.jar', 'org.hsqldb.sample.Testdb']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(levelname)s @ [%(asctime)s.%(msecs)03d] - %(message)s', datefmt='%H:%M:%S')
ch.setFormatter(formatter)
logger.addHandler(ch)


def tbl2sql(name, pfi, pfo, flag='w'):
    with open(pfi, newline='') as tbl, open(pfo, flag) as out:
        reader = csv.reader(tbl, delimiter='|')
        for row in reader:
            out.write('INSERT INTO {0} VALUES ({1})'.format(
                name.upper(), ', '.join("'{0}'".format(i) for i in row[:-1])) + os.linesep)
    os.remove(pfi)


def del2sql(pfi, pfo, flag='w'):
    with open(pfi, newline='') as de, open(pfo, flag) as out:
        reader = csv.reader(de, delimiter='|')
        for row in reader:
            out.write('DELETE FROM LINEITEM WHERE L_ORDERKEY = {}'.format(
                ''.join("'{}'".format(i) for i in row[:-1])) + os.linesep)
            out.write('DELETE FROM ORDERS WHERE O_ORDERKEY = {}'.format(
                ''.join("'{}'".format(i) for i in row[:-1])) + os.linesep)
    os.remove(pfi)


def runqgen(prefix, id, flag='w'):
    with open(os.path.join(prefix, 'q{}.sql').format(id), flag) as f:
        subprocess.call(['./qgen', str(id)], cwd=prefix, stdout=f)
    os.remove(os.path.join(prefix, '{}.sql'.format(id)))


def tpch2out(*files):
    [shutil.copy(os.path.join(__TPCH_DBGEN_PATH__, f), __OUTPUT_PATH__)
     for f in files]


def rmout(*files):
    [os.remove(os.path.join(__OUTPUT_PATH__, f)) for f in files]


def solveq15():
    st = None
    with open(os.path.join(__OUTPUT_PATH__, 'q15.sql')) as f:
        st = f.read()
    ls = st.split('\n\n')
    i = 1
    for l in ls[1:]:
        with open(os.path.join(__OUTPUT_PATH__, 'q15.' + str(i) + '.sql'), 'w') as f:
            f.write(l)
        i += 1


def run():
    path = os.path.join(__OUTPUT_PATH__, str(time.time()))
    os.mkdir(path)
    with open(os.path.join(path, 'javastdout.txt'), 'w') as f:
        subprocess.call(__JAVA_CMD__, cwd=path, stdout=f)
    return calc(os.path.join(path, 'javastdout.txt'))


def calc(path):
    res = 1.0
    with open(path) as f:
        ls = f.readlines()
        for l in ls[1:]:
            res *= float(l.split(':')[1]) / 1000.0
    res = res ** (1.0 / 22)
    res = 3600 * float(__SCALE_FACTOR__) / res
    return res


def main():
    if not os.path.exists(__OUTPUT_PATH__):
        os.mkdir(__OUTPUT_PATH__)
    if not os.path.exists(__TPCH_DBGEN_PATH__):
        raise

    queries = []
    [queries.append('queries/{}.sql'.format(i)) for i in range(1, 23)]
    tpch2out('dss.ddl', 'dss.ri', 'dbgen', 'dists.dss', 'qgen', *queries)

    logger.info('Running ./dbgen')
    subprocess.call(['./dbgen', '-v', '-s', __SCALE_FACTOR__],
                    cwd=__OUTPUT_PATH__)

    logger.info('Running ./dbgen -U')
    subprocess.call(['./dbgen', '-v', '-s', __SCALE_FACTOR__,
                     '-U', __UPDATE_FACTOR__], cwd=__OUTPUT_PATH__)

    logger.info('Convert .tbl to .sql')
    [tbl2sql(n, os.path.join(__OUTPUT_PATH__, n + '.tbl'),
             os.path.join(__OUTPUT_PATH__, n + '.sql')) for n in __NAME_LIST__]

    logger.info('Convert .u/delete to .sql')
    [tbl2sql(n, os.path.join(__OUTPUT_PATH__, n + '.tbl.u1'), os.path.join(
        __OUTPUT_PATH__, 'RF1.sql'), flag='a') for n in ['orders', 'lineitem']]
    del2sql(os.path.join(__OUTPUT_PATH__, 'delete.1'),
            os.path.join(__OUTPUT_PATH__, 'RF2.sql'))

    logger.info('Generate queries')
    [runqgen(__OUTPUT_PATH__, id) for id in range(1, 23)]

    rmout('dbgen', 'qgen', 'dists.dss')

    logger.info('Refinement')
    solveq15()

    if '__JAVA_CMD__' in globals():
        logger.info('>>> Java Test Running!')
        res = run()
        logger.info('>>> TPC-H Power = {}'.format(res))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-run', action='store_true',
                        default=False, help='Run Java only')
    args = parser.parse_args()
    if args.run:
        logger.info('>>> Java Test Running!')
        res = run()
        logger.info('>>> TPC-H Power = {}'.format(res))
    else:
        main()
