import numpy as np
import matplotlib.pyplot as plt


sig = np.zeros(100, np.float)
sig[30:60] = 1
sigdelta = sig[1:]
sigdiff = sigdelta - sig[:-1]
sigon = np.clip(sigdiff, 0, np.inf)
fig, ax = plt.subplot


fig, ax = plt.subplots()
ax.plot(sig)        
ax.set_ylim(-0.1,1.1)
plt.show() 