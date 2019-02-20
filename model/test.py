from utils.util import get_question_list
from datasketch import MinHash, MinHashLSH
import pkuseg


stop_words = set(['的', '我', '你', '我们', '他们', '你们', '是'])
data1 = ['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
        'estimating', 'the', 'similarity', 'between', 'datasets']
data2 = ['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
        'estimating', 'the', 'similarity', 'between', 'documents']

m1, m2 = MinHash(num_perm=1024), MinHash(num_perm=1024)
for d in data1:
    m1.update(d.encode('utf-8'))
for d in data2:
    m2.update(d.encode('utf-8'))
print("Estimated Jaccard for data1 and data2 is", m1.jaccard(m2))

s1 = set(data1)
s2 = set(data2)
actual_jaccard = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
print("Actual Jaccard for data1 and data2 is", actual_jaccard)

question_list = get_question_list("FQ_WEEK.csv")
# print(list(jieba.cut(question_list[0])))
# print(question_list[:5])

set1 = set(['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'datasets'])
set2 = set(['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])
set3 = set(['minhash', 'is', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])

m1 = MinHash(num_perm=128)
m2 = MinHash(num_perm=128)
m3 = MinHash(num_perm=128)
for d in set1:
    m1.update(d.encode('utf8'))
for d in set2:
    m2.update(d.encode('utf8'))
for d in set3:
    m3.update(d.encode('utf8'))

lsh = MinHashLSH(threshold=0.35, num_perm=1024)
# lsh.insert("m2", m2)
# lsh.insert("m3", m3)
# result = lsh.query(m1)
# print("Approximate neighbours with Jaccard similarity > 0.5", result)
# print(m1.jaccard(m2))
# print(m1.jaccard(m3))

# for m in result:
#     print(m1.jaccard(m))
seg = pkuseg.pkuseg()
question_list = ["这张卡能不能网购", "这张卡我要网上购物", "这张卡要开通网银", "这张卡怎么开通网银", "我的卡能不能开通网银"]
i = 0
for question in question_list:
    temp_set = set(seg.cut(question))
    intersection_set = temp_set & stop_words
    temp_set = temp_set - intersection_set
    print(temp_set)
    m = MinHash(num_perm=1024)
    for d in temp_set:
        m.update(d.encode('utf8'))
    lsh.insert(str(i), m)
    i += 1
test_question = set(seg.cut(question_list[0]))
intersection_set = test_question & stop_words
test_question = test_question - intersection_set
m = MinHash(num_perm=1024)
for d in test_question:
    m.update(d.encode("utf8"))
result = lsh.query(m)
result_jaccard = []
print("Approximate neighbours with Jaccard similarity > 0.5", result)
for index in result:
    print(question_list[int(index)])
    test_question = set(seg.cut(question_list[int(index)]))
    intersection_set = test_question & stop_words
    test_question = test_question - intersection_set
    m_temp = MinHash(num_perm=1024)
    for d in test_question:
        m_temp.update(d.encode("utf8"))
    result_jaccard.append(m.jaccard(m_temp))
    print(m.jaccard(m_temp))
#['m0', 'm1398', 'm5', 'm10', 'm9', 'm8', 'm2542', 'm2534', 'm2541', 'm1928', 'm2855']
