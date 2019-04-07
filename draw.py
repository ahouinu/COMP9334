'''
For drawing figures
Author: Tianpeng Chen
Note: This program depends on Matplotlib, so it may not be able to run on CSE machines
'''

import matplotlib.pyplot as plt
from simu import simulate
from numpy import exp, random


def draw_inter_arrivals(inter_arrival_list):
    fig, ax = plt.subplots()
    n, bins, patchs =  plt.hist(inter_arrival_list, bins=50)
    y = 0.35 * exp(-0.35 * bins) * 2000
    ax.plot(bins, y, '--')
    ax.set_xlabel('Inter-Arrival Time, lambda=0.35')
    ax.set_title('Histogram of 3500 exponentially distributed inter-arrival times')
    fig.tight_layout()
    plt.show()


def draw_service_times(services):
    fig, ax = plt.subplots()
    n, bins, patchs = plt.hist(services, bins=50)
    y = 1 * exp(-1 * bins) * 7200
    ax.plot(bins, y, '--')
    ax.set_xlabel('Service Time (Exponentially Generated), mu=1')
    ax.set_title('35000 exponentially distributed numbers (for calculating service time)')
    fig.tight_layout()
    plt.show()


def draw_real_service_times(services):
    fig, ax = plt.subplots()
    n, bins, patchs = plt.hist(services, bins=100)
    mu = 1
    y = (-mu**2 * bins**2) / 2 * exp(-mu*bins) - mu*bins*exp(-mu*bins) + 1 - exp(-mu * bins)
    ax.plot(bins, y, '--')
    ax.set_xlabel('Service Time (Calculated')
    ax.set_title('Histogram of 3500 service times in phase-type distribution')
    fig.tight_layout()
    plt.show()
    # plt.hist(services, bins=100)
    # plt.xlabel('Service Time (Calculated)')
    # plt.title('Histogram of 3500 service times in phase-type distribution')
    # plt.show()


def draw_mrt(mrts):
    plt.plot(mrts)
    plt.ylabel('Mean Response Time')
    plt.show()


def main():
    res = simulate(0, 'random', 0.35, 1, 5, 5, 10, 10000, output=False, seed=100)
    random.seed(0)
    draw_inter_arrivals(res[1])
    draw_service_times(res[2])
    draw_real_service_times(res[3])
    mrts = []
    for i in range(100):
        res = simulate(0, 'random', 0.35, 1, 5, 5, 10, 10000, output=False, seed=0)
        mrts.append(res[0])
    draw_mrt(mrts)
    print(sum(res[1]) / len(res[1]), 1/0.35)
    print(sum(res[2]) / len(res[2]), 1/1)


if __name__ == '__main__':
    main()
