import numpy as np

def simulate_price(current_price, mu=0.1, sigma=0.2, dt=1/252):
    return current_price * np.exp((mu-0.5 * sigma**2)*dt + sigma*np.sqrt(dt)*np.random.normal())
    