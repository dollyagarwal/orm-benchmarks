import time

start = time.time()

exec(open("test_a.py").read())
exec(open("test_b.py").read())
exec(open("test_d.py").read())
exec(open("test_e.py").read())
exec(open("test_f.py").read())
exec(open("test_g.py").read())
exec(open("test_h.py").read())
exec(open("test_i.py").read())
exec(open("test_j.py").read())
exec(open("test_k1.py").read())
exec(open("repopulate_customer.py").read())
exec(open("test_k2.py").read())
exec(open("test_Query1.py").read())
exec(open("test_Query2.py").read())
exec(open("test_Query3.py").read())

now = time.time()

print("Total time taken for Performance Test is {}".format(now - start))
