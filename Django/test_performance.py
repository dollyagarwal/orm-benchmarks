import test_inserts
import test_d
import test_e
import test_f
import test_g
import test_h
import test_i
import test_j
import test_k
import test_queries
import sys
import time

file_path = 'outputs/test_performance.txt'
sys.stdout = open(file_path, "w+")

if __name__ == '__main__':

    print("Performance test done at: ", time.ctime())
    print("------------Test insertions (A-C)---------------")
    test_inserts.main()
    print("------------Test D---------------")
    test_d.main()
    print("------------Test E---------------")
    test_e.main()
    print("------------Test F---------------")
    test_f.main()
    print("------------Test G---------------")
    test_g.main()
    print("------------Test H---------------")
    test_h.main()
    print("------------Test I---------------")
    test_i.main()
    print("------------Test J---------------")
    test_j.main()
    print("------------Test K---------------")
    test_k.main()

    print("------------Test Queries---------------")
    test_queries.main()