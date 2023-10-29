import numpy as np
import pandas as pd
import datetime as dt
import scipy.signal as sg
import matplotlib.pyplot as plt

from matplotlib import rcParams
from matplotlib.dates import DateFormatter


def plot_awesome(mat_contents0, fname):
    #
    # plot awesome data (.mat)
    # depends on fname format,
    #   plot narrowband data amplitude (file ends at 'A')
    #   plot narrowband data phase (file ends at 'B')
    #   plot broadband data spectogram
    #
    rcParams['figure.figsize'] = 7, 4
    rcParams['figure.autolayout'] = True
    rcParams['font.size'] = 12

    #
    # narrowband
    # -----------------------------------------------------------------------------

    if len(fname) == 26:  # é narrowband
        channel_sampling_freq0 = mat_contents0['Fs']
        data_amp = mat_contents0['data']
        callsign0 = fname[14:17]
        adc_channel0 = mat_contents0['adc_channel_number']
        start_day0 = mat_contents0['start_day']
        start_hour0 = mat_contents0['start_hour']
        start_minute0 = mat_contents0['start_minute']
        start_month0 = mat_contents0['start_month']
        start_second0 = mat_contents0['start_second']
        start_year0 = mat_contents0['start_year']
        station_name0 = mat_contents0['station_name']

        startdate0 = dt.datetime(start_year0[0, 0], start_month0[0, 0], start_day0[0, 0], start_hour0[0, 0],
                                 start_minute0[0, 0], start_second0[0, 0])

        time0 = pd.date_range(str(startdate0), periods=len(data_amp),
                              freq=str(channel_sampling_freq0)[2:3] + ' s')

        # 'Type_ABCDF':       [21,21],
        # A is low resolution (1 Hz sampling rate) amplitude
        # B is low resolution (1 Hz sampling rate) phase
        # C is high resolution (50 Hz sampling rate) amplitude
        # D is high resolution (50 Hz sampling rate) phase
        # F is high resolution (50 Hz sampling rate) effective group delay

        plot_AB = fname[21]
        if (plot_AB != 'A') and (plot_AB != 'B'):  # por hora só suporte A e B (low frequency)
            return_code = 500  # error
            return None, return_code

        if plot_AB == 'A':  # amplitude
            df0 = pd.DataFrame(data_amp, index=time0, columns=['amp'])

        if plot_AB == 'B':  # phase

            # correct phase...
            # -------------------------------------------------------------------------
            AveragingLengthAmp = 1  # dados a cada 10seg
            AveragingLengthPhase = 1
            PhaseFixLength = 60
            averaging_length = channel_sampling_freq0 * PhaseFixLength

            data_phase_fixed180 = fix_phasedata180(data_amp, averaging_length)
            data_phase_fixed190 = fix_phasedata90(data_phase_fixed180, averaging_length)

            offset = 0
            data_phase_unwrapped = np.zeros(len(data_phase_fixed190))
            data_phase_unwrapped[0] = data_phase_fixed190[0]

            for jj in range(1, len(data_phase_fixed190)):
                if data_phase_fixed190[jj] - data_phase_fixed190[jj - 1] > 180:
                    offset = offset + 360
                elif data_phase_fixed190[jj] - data_phase_fixed190[jj - 1] < -180:
                    offset = offset - 360
                data_phase_unwrapped[jj] = data_phase_fixed190[jj] - offset

            df0 = pd.DataFrame(data_phase_unwrapped, index=time0, columns=['phase'])

        df0_integrated = df0.resample('10 s').mean()  # dado de amplitude a cada 10 segundos

        try:
            fig, (ax0) = plt.subplots(1)  # , sharex=True, sharey=False

            if plot_AB == 'A':
                ax0.plot(df0_integrated, 'b:', lw=2, alpha=0.4, label='10s sampling')
                ax0.plot(df0_integrated.resample('60 s').mean(), alpha=0.8, color='black', lw=1, label='60s sampling')

            if plot_AB == 'B':
                ax0.plot(df0_integrated, color='darkblue', lw=1.5, alpha=0.9, label='10s sampling')

            ax0.set_xlabel('Time (UT Hours)')
            ax0.xaxis.set_major_formatter(DateFormatter('%H:%M'))
            ax0.grid(True)

            if plot_AB == 'A':
                ax0.set_ylim(0, np.nanmax(df0_integrated) * 1.05)
                sub_title = 'Amplitude'
                ax0.set_ylabel('Averaged Amplitude [dB]')
            else:
                ax0.set_ylim(np.nanmin(df0_integrated) - 100, np.nanmax(df0_integrated) + 100)
                sub_title = 'Phase'
                ax0.set_ylabel('Averaged Phase [degrees]')

            if adc_channel0 == 0:
                ch = 'N/S'
            elif adc_channel0 == 1:
                ch = 'E/W'
            else:
                ch = ''

            ax0.set_title(
                ''.join(map(lambda num: chr(num[0]), station_name0)) + ' ' + str(startdate0)[0:10] + ' ' + str(
                    callsign0) + ' ' + sub_title + ', ' + ch + ' Antenna', weight='bold')

            plt.legend(fontsize=8)

            plt.show()

            return_code = 0

        except:
            return_code = 550  # error

        return fig, return_code

    #
    # broadband
    # -----------------------------------------------------------------------------

    if len(fname) == 22:  # é broadband

        channel_sampling_freq0 = mat_contents0['Fs']
        data_amp = mat_contents0['data']
        callsign0 = fname[14:17]
        adc_channel0 = mat_contents0['adc_channel_number']
        start_day0 = mat_contents0['start_day']
        start_hour0 = mat_contents0['start_hour']
        start_minute0 = mat_contents0['start_minute']
        start_month0 = mat_contents0['start_month']
        start_second0 = mat_contents0['start_second']
        start_year0 = mat_contents0['start_year']
        station_name0 = mat_contents0['station_name']

        startdate0 = dt.datetime(start_year0[0, 0], start_month0[0, 0], start_day0[0, 0], start_hour0[0, 0],
                                 start_minute0[0, 0], start_second0[0, 0])

        time0 = pd.date_range(str(startdate0), periods=len(data_amp),
                              freq=str(channel_sampling_freq0)[2:3] + ' s')

        df0 = pd.DataFrame(data_amp, index=time0, columns=['amp'])
        #    df0_integrated = df0.resample('10 s').mean()    # dado de amplitude a cada 10 segundos

        try:
            rcParams['figure.figsize'] = 7.5, 4.5
            fig, (ax0) = plt.subplots(1)  # , sharex=True, sharey=False

            # Plot the spectrogram
            ax0.specgram(df0.amp, Fs=94000)

            pcm = ax0.pcolormesh(np.random.random((20, 20)), cmap='viridis')

            fig.colorbar(pcm, label='Intensity (dB)', ax=ax0)
            ax0.set_xlabel('Time (s)')
            ax0.set_ylabel('Frequency (Hz)')

            sub_title = 'Spectogram'
            ch = ''
            ax0.set_title(
                ''.join(map(lambda num: chr(num[0]), station_name0)) + ' ' + str(startdate0)[0:10] + ' ' + str(
                    callsign0) + ' ' + sub_title + ', ' + ch + ' Antenna', weight='bold')

            plt.show()

            return_code = 0

        except:
            return_code = 550  # error

        return fig, return_code


def fix_phasedata180(data_phase, averaging_length):
    #
    # return fix phase data 180 ONLY AWESOME data
    #

    data_phase = np.reshape(data_phase, len(data_phase))
    x = np.exp(1j * data_phase * 2. / 180. * np.pi)
    N = float(averaging_length)
    b, a = sg.butter(1, 0.021)
    y = sg.filtfilt(b, a, x)
    output_phase = data_phase - np.round(
        ((data_phase / 180 * np.pi - np.unwrap(np.angle(y)) / 2) % (2 * np.pi)) * 180 / np.pi / 180) * 180
    temp = output_phase[0] % 90
    output_phase = output_phase - output_phase[0] + temp
    s = output_phase[output_phase >= 180]
    for s in range(len(output_phase)):
        output_phase[s] = output_phase[s] - 360
    return output_phase


def fix_phasedata90(data_phase, averaging_length):
    #
    # return fix phase data 90 ONLY AWESOME data
    #

    data_phase = np.reshape(data_phase, len(data_phase))
    x = np.exp(1j * data_phase * 4. / 180. * np.pi)
    N = float(averaging_length)
    b, a = sg.butter(1, 0.021)
    y = sg.filtfilt(b, a, x)
    output_phase = data_phase - np.round(
        ((data_phase / 180 * np.pi - np.unwrap(np.angle(y)) / 4) % (2 * np.pi)) * 180 / np.pi / 90) * 90
    temp = output_phase[0] % 90
    output_phase = output_phase - output_phase[0] + temp
    output_phase = output_phase % 360
    s = output_phase[output_phase >= 180]
    for s in range(len(output_phase)):
        output_phase[s] = output_phase[s] - 360
    return output_phase
