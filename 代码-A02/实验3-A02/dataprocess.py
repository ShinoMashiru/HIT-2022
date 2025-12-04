def edge_csv():
    f = open('C:/Users/dell/Desktop/data/0.edges').readlines()
    out = open('C:/Users/dell/Desktop/data/edges.csv', 'w')
    print('from_id,to_id', file=out)
    nodes = range(1, 347)
    for line in f:
        tmp = line.strip().split(' ')
        print(tmp[0] + ',' + tmp[1], file=out)
    for node in nodes:
        print('0,' + str(node), file=out)
        print(str(node) + ',0', file=out)

def feature_extract():
    feature_file = open('C:/Users/dell/Desktop/data/0.featnames').readlines()
    feats_file = open('C:/Users/dell/Desktop/data/0.feat').readlines()
    feat_out = open('C:/Users/dell/Desktop/data/feat.csv', 'w')
    print('id', file=feat_out, end='')
    features = {}
    length = {}
    for line in feature_file:
        tmp = line.strip().split(' ')
        feature_name, feature_id = tmp[1], tmp[3]
        if features.__contains__(feature_name) is False:
            features[feature_name] = list()
        features[feature_name].append(feature_id)
    for feature in features:
        length[feature] = len(features[feature])
        print(',' + feature, file=feat_out, end='')
    print(file=feat_out)
    for person in feats_file:
        tmp = person.strip().split(' ')
        node_id = tmp[0]
        print(node_id, file=feat_out, end='')
        now = 1
        c_list = []
        for feature in features:
            feat = tmp[now : now + length[feature]]
            count = -1
            for i in range(length[feature]):
                if feat[i] != '0':
                    count = i
            if count > -1:
                print(',' + features[feature][count], file=feat_out, end='')
            if count == -1:
                print(',' + '-1', file=feat_out, end='')
            now += length[feature]
        assert now == 225
        print(file=feat_out)

def circle():
    circle_file = open('C:/Users/dell/Desktop/data/0.circles').readlines()
    out = open('C:/Users/dell/Desktop/data/circles.csv', 'w')
    for line in circle_file:
        line = line.strip()
        line = line.replace('\t', ' ')
        l = len(line)
        for i in range(l):
            if line[i] == ' ':
                tmp = list(line)
                tmp[i] = ','
                print(''.join(tmp), file=out)
                break

def check():
    f = open('C:/Users/dell/Desktop/data/0.edges').readlines()
    edge = {}
    for line in f:
        tmp = line.strip().split(' ')
        tmp = (min(tmp), max(tmp))
        if edge.__contains__(tmp) is not True:
            edge[tmp] = 1
        else:
            edge[tmp] += 1
    for tmp in edge:
        assert edge[tmp] == 2
    print('all node have doble edge.')

if __name__ == '__main__':
    edge_csv()
    feature_extract()
    circle()

# load csv with headers from "file:///feat.csv" as line
'''
load csv with headers from "file:///feat.csv" as line
create (n:Person{id:toInteger(line.id)})

load csv with headers from "file:///edges.csv" as line
match (from:Person{id:toInteger(line.from_id)}), (to:Person{id:toInteger(line.to_id)})
merge (from)-[:Be_Friend_With]->(to)

load csv with headers from "file:///feat.csv" as line
match (n:Person{id:toInteger(line.id)})
set n.birthday = toInteger(line.birthday)
set n.languages_id = toInteger(line.languages_id)
set n.education_year_id = toInteger(line.education_year_id)
set n.gender = toInteger(line.gender)
set n.work_with_id = toInteger(line.work_with_id)
set n.work_location_id = toInteger(line.work_location_id)
set n.last_name = toInteger(line.last_name)
set n.locale = toInteger(line.locale)
set n.education_type = toInteger(line.education_type)
set n.work_start_date = toInteger(line.work_start_date)
set n.location_id = toInteger(line.location_id)
set n.education_with_id = toInteger(line.education_with_id)
set n.education_degree_id = toInteger(line.education_degree_id)
set n.work_position_id = toInteger(line.work_position_id)
set n.work_end_date = toInteger(line.work_end_date)
set n.education_school_id = toInteger(line.education_school_id)
set n.education_concentration_id = toInteger(line.education_concentration_id)
set n.work_employer_id = toInteger(line.work_employer_id)
set n.education_classes_id = toInteger(line.education_classes_id)
set n.first_name = toInteger(line.first_name)
set n.hometown_id = toInteger(line.hometown_id)
set n.circle = []

load csv from "file:///circles.csv" as line
UNWIND split(line[1], ' ') as x
match(n:Person{id:toInteger(x)})
set n.circle = n.circle + line[0]
'''