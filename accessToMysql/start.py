import create_table as mct
import access_to_csv as mcts
import csvToMysql as mctm



accdb_path_arr = [
                    # 'C:/Users/09/Desktop/access/bombay/202402-EXP-2108.accdb',
                    # 'C:/Users/09/Desktop/access/bombay/202402-IMP-2107.accdb',
                    # 'C:/Users/09/Desktop/access/ecuador/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/ecuador/202402-IMP.accdb',
                    # 'C:/Users/09/Desktop/access/ethiopia/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/ethiopia/202402-IMP.accdb',
                    # 'C:/Users/09/Desktop/access/kazakhstan/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/kazakhstan/202402-IMP-1.accdb',
                    # 'C:/Users/09/Desktop/access/pakistan/202402-EXP-FULL.mdb',
                    # 'C:/Users/09/Desktop/access/pakistan/202402-IMP-FULL.mdb',
                    # 'C:/Users/09/Desktop/access/panama/202402-EXP-FULL.mdb',
                    # 'C:/Users/09/Desktop/access/panama/202402-IMP-FULL.mdb',
                    # 'C:/Users/09/Desktop/access/paraguay/202402-EXP-RAW.mdb',
                    # 'C:/Users/09/Desktop/access/paraguay/202402-IMP-RAW.mdb',
                    # 'C:/Users/09/Desktop/access/peru/202402-IMP.accdb',
                    # 'C:/Users/09/Desktop/access/peru/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/philippines/PH_EXPORT_202402_RAW.accdb',
                    # 'C:/Users/09/Desktop/access/philippines/PH_IMPORT_202402_RAW.accdb',
                    # 'C:/Users/09/Desktop/access/uganda/UG0010_IMP_202402.accdb',
                    # 'C:/Users/09/Desktop/access/uganda/UG0020_EXP_202402.accdb',
                    # 'C:/Users/09/Desktop/access/ukraine/UA_IMPORT_202402.accdb',
                    # 'C:/Users/09/Desktop/access/uruguay/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/uruguay/202402-IMP.accdb',
                    # 'C:/Users/09/Desktop/access/uzbekistan/202402-EXP.accdb',
                    # 'C:/Users/09/Desktop/access/uzbekistan/202402-IMP.accdb',
                    'C:/Users/09/Desktop/access/mexico/202402-EXP.mdb',
                    # 'C:/Users/09/Desktop/access/mexico/202402-IMP.mdb',
                  ] # Access数据库路径


# 遍历每个 ACCDB 文件路径
for accdb_path in accdb_path_arr:
    # 依次执行处理方法
    mct.create_table(accdb_path)
    mcts.access_to_csv(accdb_path)
    mctm.csv_to_mysql(accdb_path)