import pandas as pd
import matplotlib.pyplot as plt

from matplotlib import rcParams
from matplotlib.dates import DateFormatter


def plot_savnet(mat_contents0, fname):
    fx = mat_contents0
    header = []
    for a in fx[0].header.values():
        header.append(str(a))
    source = header[0:8]
    header = header[8::]

    fig, ax = plt.subplots(1, 2, figsize=(16, 5))

    try:
        rcParams['figure.figsize'] = 16, 5
        rcParams['figure.autolayout'] = True
        rcParams['font.size'] = 10
        rcParams['xtick.labelsize'] = 10

        t = pd.date_range(fx[0].header['DATE-OBS'], periods=fx[0].header['NAXIS2'], freq='s')
        df = pd.DataFrame(fx[0].data.byteswap().newbyteorder(), columns=header,
                          index=t)  # https://github.com/astropy/astropy/issues/1156

        df = df.resample('60 s').mean()

        for name in [x for x in header if 'Amp' in x]:
            ax[0].plot(df[name], label=name, lw=1, alpha=0.9)

        for name in [x for x in header if 'Phase' in x]:
            ax[1].plot(df[name], label=name, lw=1, alpha=0.9)

        ax[0].set_title(source[-1].upper() + ' - ' + source[-2] + ' - Amplitude', weight='bold', fontsize=16)
        ax[1].set_title(source[-1].upper() + ' - ' + source[-2] + ' - Phase', weight='bold', fontsize=16)
        ax[0].set_xlabel(header[0] + ' [sample 60s]', fontsize=12)
        ax[1].set_xlabel(header[0] + ' [sample 60s]', fontsize=12)
        ax[0].set_ylabel('Averaged Amplitude [dB]', fontsize=12)
        ax[1].set_ylabel('Averaged Phase [degrees]', fontsize=12)

        ax[0].xaxis.set_major_formatter(DateFormatter('%H:%M'))
        ax[1].xaxis.set_major_formatter(DateFormatter('%H:%M'))

        ax[0].legend(loc='best', fontsize=9)
        ax[1].legend(loc='best', fontsize=9)
        ax[0].grid()
        ax[1].grid()

        plt.tight_layout()
        plt.show()

        rc = 0

    except:
        rc = 255  # error

    return fig, rc
