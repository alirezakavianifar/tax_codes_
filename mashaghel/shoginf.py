import pyodbc


lists = input()

lists = lists.split(',')


def pairwise(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(a, a)


for cod_hozeh, k_parvand in pairwise(lists):
    # k_parvandes = lists[1::2]
    # cod_hozes = lists[0::2]
    server = 'SQL Server'
    database = 'MASHAGHEL'
    username = 'mash'
    password = '123456'

    sql = """\
    update [MASHAGHEL].[dbo].[shog_inf]
    set Code_Parvandeh = NULL
    where cod_hozeh={0} and k_parvand = {1}  
    """.format(cod_hozeh, str("""'%s/000'""" % k_parvand))
    print(sql)
    try:
        cnxn = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=10.52.0.50\AHWAZ' + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()

        cursor.execute(sql)
        cursor.execute('commit')
        print('done')
    except:
        print("error")
