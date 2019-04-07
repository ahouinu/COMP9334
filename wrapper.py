'''
COMP9334 Project Wrapper Program
Author: Tianpeng Chen
'''
import sys
import time
import argparse

from simu import simulate, SimulationFailure

root = './test/'


class Test:

    def __init__(self, id, mode, arrival, service, params=[]):
        self.id = id
        self.mode = mode
        self.params = params
        if mode == 'trace':
            self.arrival = arrival
            self.service = service
        elif mode == 'random':
            self.arrival = arrival[0]
            self.service = service[0]
        # try:
        #     self.nb_of_servers = params[0]
        #     self.setup_time = params[1]
        #     self.delayed_off_time = params[2]
        # except IndexError:
        #     pass

    def run(self, time_end=100, seed=0, output=True):
        print(f'running test {self}')
        res = simulate(self.id, self.mode, self.arrival, self.service, int(self.params[0]), self.params[1], self.params[2], time_end, seed, output=output)
        return res

    def __str__(self):
        res = ''
        for k, v in self.__dict__.items():
            res += f'{k}: {v}\t'

        return res


def main(func, params=[]):
    if func not in ('file', 'tc', 'rep'):
        print('Usage: python3 wrapper.py file, tc or rep')
        sys.exit(0)
    if func == 'file':
        run_from_file(params)
    elif func == 'tc':
        determine_tc(int(params[0]), int(params[1]))
    elif func == 'rep':
        replicate(params)


def replicate(args=[]):
    mode = 'random'
    arrival = float(args[0])
    service = float(args[1])
    nb_of_servers = int(args[2])
    setup_time = float(args[3])
    tc = float(args[4])
    seed = int(args[5])

    baseline = simulate(0, mode, arrival, service, nb_of_servers, setup_time, 0.1, time_end=1000, seed=seed, output=False)
    res = simulate(1, mode, arrival, service, nb_of_servers, setup_time, tc, time_end=1000, seed=seed, output=False)
    print('\n\n')
    print(f'baseline mrt:\t {baseline[0]}')
    print(f'mrt:\t {res[0]}')


def run_from_file(args=[]):
    with open(root + 'num_tests.txt', 'r') as f:
        nb_of_tests = int(f.readline(1))

    modes = []
    arrivals = []
    services = []
    params = []

    tests = []

    for i in range(nb_of_tests):
        with open(root + 'mode_' + f'{i+1}' + '.txt', 'r') as f:
            for line in f:
                modes.append(str(line))
        with open(root + 'arrival_' + f'{i+1}' + '.txt', 'r') as f:
            tmp = []
            for line in f:
                tmp.append(float(line))
            arrivals.append(tmp)
        with open(root + 'service_' + f'{i+1}' + '.txt', 'r') as f:
            tmp = []
            for line in f:
                tmp.append(float(line))
            services.append(tmp)
        with open(root + 'para_' + f'{i+1}' + '.txt', 'r') as f:
            tmp = []
            for line in f:
                tmp.append(float(line))
            params.append(tmp)

    for i in range(nb_of_tests):
        tests.append(Test(i+1, modes[i], arrivals[i], services[i], params=params[i]))

    if args:
        nb = int(args[0]) - 1
        tests[nb].run()
    else:
        for test in tests:
            test.run()

    # tests[0].run()


def determine_tc(nb_of_replication, _range, step=1):
    mode = 'random'
    arrival = 0.35
    service = 1
    nb_of_servers = 5
    setup_time = 5
    TRY_TC_RANGE = _range
    time_end = 10000

    tests = []
    title = f'Test Header: {nb_of_replication}\ttc range [1, {TRY_TC_RANGE})\t{time_end}\n'
    header = 'seed\ttest_id\ttc\tmrt\n'
    results = []

    for i in range(1, TRY_TC_RANGE):
        tests.append(Test(i, mode, [arrival], [service], params=[nb_of_servers, setup_time, i]))

    seed_list = [_ for _ in range(nb_of_replication)]
    for seed in seed_list:
        baseline = Test(0, mode, [arrival], [service], params=[nb_of_servers, setup_time, 0.1])
        b_mrt = baseline.run(time_end, seed, output=False)[0]
        results.append((seed, 'baseline', 0.1, b_mrt))
        for test in tests:
            try:
                mrt = test.run(time_end, seed, output=False)[0]
            except SimulationFailure:
                print('Try again after 5 seconds...')
                time.sleep(5)
                new_test = Test(test.id, test.mode, [test.arrival], [test.service], test.params)
                mrt = new_test.run(time_end, seed, output=False)[0]
            results.append((seed, test.id, test.params[2], mrt))

    with open('./test/tc_log_range_50_step_1_for_plot.txt', 'w') as f:
        f.write(title)
        f.write(header)
        for r in results:
            f.write('\t'.join([str(e) for e in r]))
            f.write('\n')

    print(results)


def analyse_tc_data(filename):
    data = []
    count = 0
    tmp = []
    f = open(filename, 'r')
    for _ in range(2):
        next(f)
    for line in f:
        if count > 15:
            data.append(tmp)
            count = 0
            tmp = []
        _str = line.strip('\n').split('\t')
        tmp.append(float(_str[3]))
        count += 1

    mmrt = []
    for j in range(len(data[0])):
        tmp = 0
        for i in range(len(data)):
            tmp += data[i][j]
        tmp /= len(data[0])
        mmrt.append(tmp)

    print(mmrt)


if __name__ == '__main__':
    func = input('Type function: file, tc, or rep\n')
    print('Provide additional parameters for tc or rep\n')
    print('For file, it will be the sample test you want to run (leave empty for running all)')
    print('For tc, it will be the number of replications and range of tc')
    print('For rep, it will be arrival, service, number of servers, setup time, delayed off time and seed')
    params = input('Type parameters here: ').split()
    main(func, params)
    # analyse_tc_data('./test/tc_log_range_50_step_1_for_plot.txt')
