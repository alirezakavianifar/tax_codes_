import glob
import os


def rename_files(path,
                 dest,
                 prefix='.xls',
                 postfix='.html',
                 file_list=None,
                 *args,
                 **kwargs):
    if file_list is None:
        file_list = glob.glob(path + "/*" + prefix)

    for i, item in enumerate(file_list):
        name = item.rsplit('\\', 1)[-1].split('.')[0]
        if len(name) == 8:
            name = f"00{name}"
        if len(name) == 9:
            name = f"0{name}"
        dest1 = os.path.join(dest, f"{name}.jpg")
        os.rename(item, dest1)


rename_files(path=r'C:\Users\alkav\Desktop\ForImportEtebarReport\ExelFiles\Eblagh\ExcelFiles\oldaks',
             dest=r'C:\Users\alkav\Desktop\ForImportEtebarReport\ExelFiles\Eblagh\ExcelFiles\newaks',
             prefix='.jpg',
             postfix='.jpg')
