# predeal the user_data

infile = open(r"D:\02\JData_User.csv", 'r')
outfile = open(r"D:\02\1-data\user.csv", 'w')

count = 0
error = 0
outfile.write(','.join(['user_id', 'age_left', 'age_right', 'sex', 'user_lv_cd', 'user_reg_dt']) + '\n')
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
        outfile.write(','.join(outline) + '\n')
infile.close()
outfile.close()


# predeal the action_data

period = ['_201602', '_201603', '_201603_extra', '_201604']
outfile = open(r"D:\02\1-data\action.csv", 'w')
outfile.write('user_id,sku_id,time,model_id,type,cate,brand\n')

count = 0
for time in period:
    infile = open('D:/02/JData_Action%s.csv' % time, 'r')
    line = infile.readline()
    line = infile.readline()
    while line:
        count += 1
        outfile.write(line)
        line = infile.readline()
    infile.close()
outfile.close()

print "count: %d" % count

# csv to txt
'''
files = ['JData_Product', 'JData_Comment']
for file in files:
    infile = open("D:/02/%s.csv" % file, 'r')
    outfile = open('D:/02/1-data/%s.txt' % file, 'w')
    for line in infile.readlines():
        line = line.strip().split(',')
        outfile.write(','.join(line) + '\n')
    infile.close()
    outfile.close()
'''
