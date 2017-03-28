# predeal the user_data
# 'user_id', 'age_left', 'age_right', 'sex', 'user_lv_cd', 'user_reg_dt'

infile = open("C:/JData/data/JData_User.csv", 'r')
outfile = open("C:/JData/data/user.txt", 'w')

count = 0
error = 0
for line in infile.readlines():
    count += 1
    if count > 1:
        line = line.strip().split(',')
        if line[1] == '-1':
            age = ['-1', '-1']
        elif '-' in line[1]:
            age = [line[1][0:2], line[1][3:5]]
        elif line[1][0:2] == '15':
            age = ['0', '15']
        elif line[1][0:2] == '56':
            age = ['56', '100']
        else:
            error += 1
        outline = [line[0]] + age + line[2:]
        outfile.write('\t'.join(outline) + '\n')
infile.close()
outfile.close()


# predeal the action_data
# 'user_id,sku_id,time,model_id,type,cate,brand\n'

period = ['_201602', '_201603', '_201603_extra', '_201604']
outfile = open("C:/JData/data/action.txt", 'w')

count = 0
for time in period:
    infile = open('C:/JData/data/JData_Action%s.csv' % time, 'r')
    line = infile.readline()
    line = infile.readline()
    while line:
        count += 1
        line = line.strip().split(',')
        outfile.write('\t'.join(line) + '\n')
        line = infile.readline()
    infile.close()
outfile.close()

print "count: %d" % count


# csv to txt

import os
files = ['JData_Product', 'JData_Comment']
for file in files:
    infile = open("C:/JData/data/%s.csv" % file, 'r')
    outfile = open('C:/JData/data/%s.txt' % file, 'w')
    line = infile.readline()
    line = infile.readline()
    while line:
        line = line.strip().split(',')
        outfile.write('\t'.join(line) + '\n')
        line = infile.readline()
    infile.close()
    outfile.close()

os.rename('C:/JData/data/%s.txt' % 'JData_Product', 'C:/JData/data/%s.txt' % 'product')
os.rename('C:/JData/data/%s.txt' % 'JData_Comment', 'C:/JData/data/%s.txt' % 'comment')
