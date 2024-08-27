# The modules here are used for importing EPIC log files to a DataFrame and optionally export it as an *.xlsx file.
# Copyright (C) 2024  Altug Yildirim

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import glob

import pandas as pd


def epiclog_read(name):
    """
    Function to import the log files from the custom Molecular
    Beam Epitaxy program EPIC, which is used in Paul-Drude-Institut.
    Produces pandas DataFrame for each text file, with index as a timestamp.
    """
    # Read log files
    df = pd.read_csv(name, skiprows=1)

    # Replaces dots, backticks and spaces with underscores.
    df.columns = (
        df.columns.str.replace("[']", '', regex=True)
        .str.replace('[.]', '_', regex=True)
        .str.replace('[ ]', '_', regex=True)
    )

    # Converting columns named 'Date&Time' to 'Date',
    # as it is inconsistent between different log files.
    if 'Date&Time' in df.columns:
        df = df.rename({'Date&Time': 'Date'}, axis='columns')

    # Converting column 'Date' to timestamp
    df.Date = pd.to_datetime(df.Date, dayfirst=True)

    # Using timestamp as index, resampling by time requires DateTimeIndex.
    df.index = df['Date']
    df = df.drop(columns='Date')

    # Add comment and name attributes to the DataFrame from log files
    # and replace dots and spaces with underscores.
    df.comment = open(name).readlines()[0][1:].replace('.', '_')
    df.name = name[16:-4].replace('.', '_').replace(' ', '_')

    return df


def epiclog_read_batch(date, data_path):
    """
    Function to import the log files in batch from a folder.
    """
    path_list = [glob.glob(e) for e in [data_path + date + '/*.txt']][0]
    dataframe_list = [None] * len(path_list)

    for i in range(len(dataframe_list)):
        dataframe_list[i] = epiclog_read(path_list[i])

    return dataframe_list


def threshold_sampling(df, change, threshold_value=0.01):
    """
    Using pct_change/diff to find relative/absolute difference between
    two rows in a column.
    This function is designed to reduce the unnecessary data, rather than resampling.
    Yet, if you change the value threshold_value on your needs,
    you can use it as resampling anyway.
    The threshold_value 0.01 is an arbitrary value, but otherwise for loop that is used
    inside accumulated_sampling function is too heavy computational-wise.
    """
    # With pd.merge, merge the relative/absolute difference column and real value.
    # Set index to Date and delete the rows that are zero with
    # .loc[(df[i]!=0).any(axis=1)]
    # If you want to drop the NaN value row (which, the first value will also be)
    # add .dropna() to line just below.
    if change == 'relative':
        df = (
            pd.merge(df.reset_index(), df.pct_change().reset_index(), on=['Date'])
            .set_index('Date')
            .loc[(df.pct_change() != 0).any(axis=1)]
            .fillna(threshold_value + 1)
        )
    else:
        df = (
            pd.merge(df.reset_index(), df.diff().reset_index(), on=['Date'])
            .set_index('Date')
            .loc[(df.diff() != 0).any(axis=1)]
            .fillna(threshold_value + 1)
        )
    # remove the rows only below certain percentage that user gives,
    # using the additional column with relative/absolute difference values created above
    df = df[df.iloc[:, 1] >= threshold_value]
    # drop the column with relative differences after selection.
    df = df.drop(columns=df.iloc[:, 1:].columns.tolist())
    # after the merge function, the data column and the difference column are named as
    # NAME_x and NAME_y respectively, as difference column is dropped, there is no need
    # to keep the _x suffix in the end NAME.
    df.columns = df.columns.str.replace('_x', '', regex=True)
    return df


def accumulated_sampling(df, change, threshold_value):
    """
    This function is designed to use the pct_change/diff methods when there is an
    incremental change that can not be simply catched by comparing the following
    rows of a column. Imagine you have a series of data like 10.1, 10.2. 10.3,...
    If you want to apply a sampling of 2% change, eventually your end result will be
    just an empty DataFrame.
    Rather this method creates a new DataFrame(dataframe_change),
    places the first value of the existing DataFrame(df) to this new DataFrame.
    Afterwards uses the methods pct_change/diff to compare the values in df to
    the latest value in dataframe_change and if it is bigger then the threshold value
    (threshold_value) that is given by the user, the compared value of df is
    appended with Pandas method concat to dataframe_change. For the given example above,
    this can catch the change as; 10.1, 10.3, ...
    """
    # Create an empty DataFrame
    dataframe_change = pd.DataFrame()
    # Some of the EPIC file logs can be empty, control this to prevent further errors.
    if df.empty != True:
        # Append the first value of df to dataframe_change
        dataframe_change = pd.concat([dataframe_change, df.iloc[[0]]])
        # Check every row value in df with the condition that if the
        # relative/absolute difference of the last value in dataframe_change and
        # the df.iloc[j,0]] is bigger than the threshold_value, append
        # with concat to dataframe_change. This for loop is computationally very heavy
        # so it is advised that this function is used together with threshold_sampling
        # function with a relatively low threshold_value chosen(like default option of
        # 0.01).
        for j in range(len(df)):
            if change == 'relative':
                condition = (
                    abs(
                        pd.Series(
                            [dataframe_change.iloc[-1, 0], df.iloc[j, 0]]
                        ).pct_change()[1]
                    ).round(1)
                    * 100
                )
            else:
                condition = abs(
                    pd.Series([dataframe_change.iloc[-1, 0], df.iloc[j, 0]]).diff()[1]
                ).round(1)
            if condition >= threshold_value:
                dataframe_change = pd.concat([dataframe_change, df.iloc[[j]]])
        # Add the suffix _change to dataframe_change column to differentiate with
        # the column of df with the same name.
        dataframe_change = dataframe_change.add_suffix('_change')
        # merge dataframe_change to df
        df = df.join(dataframe_change)
        # delete original df column
        df = df.drop(columns=df.iloc[:, :1].columns.tolist())
        #  delete the suffix _change
        df.columns = df.columns.str.replace('_change', '', regex=True)
        # drop the empty data points.
        df = df.dropna(axis=0)
    return df


def resampling(
    dataframe_list, percent_cut, value_cut, resampling_period, resample_method='diff'
):
    """
    This DataFrame are resampled either with time intervals
    or absolute(for temperature values) or relative(for pressure values) difference.
    Default is resampling by difference.
    """
    comment_list = [None] * len(dataframe_list)
    name_list = [None] * len(dataframe_list)
    for i in range(len(dataframe_list)):
        comment_list[i] = dataframe_list[i].comment
        name_list[i] = dataframe_list[i].name
        # Resampling, either over DateTime or Relative/Absolute Change
        if resample_method == 'diff':
            # Only resize data texts(Date and additional column),
            # effectively leave out Messages and Shutter out.
            if dataframe_list[i].columns.size < 3:
                # Search for pressure related log files and create new DataFrame by using
                # relative change (dataframe.pct_change()\*100) to fill a newly created
                # DataFrame only with values over certain threshold percentages.
                # Search columns with the name inside 'IG', 'MIG' or 'PG' which gives
                # pressure related log data.
                if dataframe_list[i].filter(regex='IG|MIG|PG').columns.values.tolist():
                    dataframe_list[i] = threshold_sampling(
                        dataframe_list[i], 'relative'
                    )
                    dataframe_list[i] = accumulated_sampling(
                        dataframe_list[i], 'relative', percent_cut
                    )

                # Search for temperature related log files and create new DataFrame by using
                # absolute change (dataframe.diff()) to fill a newly created DataFrame
                # only with values over certain threshold value. Search columns with the
                # name inside 'PID' or 'Pyro' which gives temperature related log data.
                if dataframe_list[i].filter(regex='PID|Pyro').columns.values.tolist():
                    dataframe_list[i] = threshold_sampling(
                        dataframe_list[i], 'absolute'
                    )
                    dataframe_list[i] = accumulated_sampling(
                        dataframe_list[i], 'absolute', value_cut
                    )
        else:
            # resample by time to reduce the size of data arrays,
            # otherwise the dataframe combined becomes too big for the memory.

            if dataframe_list[i].columns.size == 3:
                agg_rules = {'CallerID': 'last', 'Message': 'last', 'Color': 'last'}
                dataframe_list[i] = (
                    dataframe_list[i].resample(resampling_period).agg(agg_rules)
                )
            elif dataframe_list[i].columns.size == 11:
                dataframe_list[i] = dataframe_list[i].resample(resampling_period).last()
            else:
                dataframe_list[i] = dataframe_list[i].resample(resampling_period).mean()
            # Fill the empty rows of Shutter DataFrame
            if name_list[i] == 'Shutters':
                dataframe_list[i].fillna(method='ffill', inplace=True)

        # Split the Message column to object, from and to columns
        if dataframe_list[i].filter(regex='Message').columns.values.tolist():
            dataframe_list[i] = growth_time(dataframe_list[i])
        else:
            dataframe_list[
                -1
            ].grow = 'Error: No Message log detected, can not determine the number of growth events and the start and end of the growth!'
            print(dataframe_list[-1].grow)

        # have re-run this again, metadata is lost after df = df methods
        # must be replaced with (inplace=True) method.
        dataframe_list[i].comment = comment_list[i]
        dataframe_list[i].name = name_list[i]

    return dataframe_list


def growth_time(df):
    # Drop the CallID and Color columns
    df.drop(df.columns[[0, 2]], axis=1, inplace=True)
    # Check if the Message column contains the moved objects, like mirror or holder.
    if df['Message'].str.contains('moved from').any() == True:
        # Filter the rows that contain the moved from message
        df = df[df['Message'].str.contains('moved from')]
        # Drop the rows that contain the Mirror, effectively only the holder is left.
        df = df[~df['Message'].str.contains('Mirror')]
        # When the Mirror values dropped, the DataFrame can be empty(No growth detected)
        # Check this to prevent errors.
        if df.empty != True:
            # Split the Message column to Object, From and to Columns
            df[['object', 'from']] = df.pop('Message').str.split(
                ' moved from ', expand=True
            )
            df[['from', 'to']] = df.pop('from').str.split(' to ', expand=True)
            if any(count != 2 for count in df['object'].value_counts()):
                if any(count > 1 for count in df['object'].value_counts()):
                    df.grow = 'Error: You use the same name for different growth events! Please use unique names for each growth event.'
                else:
                    # in here there needs to be a check again for the case if for the
                    # given growth event, in the from column GC comes before
                    # MC_manip, a growth started in previous day, vice versa means
                    # a growth started today and still going on.
                    df.grow = 'The number of growth events is not equal to the number of start and end of the growth events!'
            else:
                # in here there needs to be a check again for the case if for the given
                # growth event, in the from column GC comes before
                # MC_manip, which should raise an error as it means that there is
                # a user error: a growth started in previous day
                # and the it is done today and user tries to start a new growth
                # event today with the same name.
                if len(df.index) == 2:
                    df.grow = 'Single Growth detected.'
                    print(
                        'Start of the Growth: '
                        + df.index[0].strftime('%Y-%m-%d %H:%M:%S')
                    )
                    print(
                        'End of the Growth: '
                        + df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
                    )
                if len(df.index) % 2 == 0:
                    df.grow = (
                        str(df['object'].value_counts()[0])
                        + ' growths detected with the names '
                        + ','.join(df['object'].value_counts().index.values.tolist())
                        + '.'
                    )
                    for i in range(0, len(df.index), 2):
                        print(
                            'Start of the Growth '
                            + df.iloc[i, 0]
                            + ':'
                            + df.index[i].strftime('%Y-%m-%d %H:%M:%S')
                        )
                        print(
                            'End of the Growth '
                            + df.iloc[i + 1, 0]
                            + ':'
                            + df.index[i + 1].strftime('%Y-%m-%d %H:%M:%S')
                        )
        else:
            df.grow = 'No growth detected.'
    print(df.grow)
    return df


def epic_xlsx(date, data_path, dataframe_list):
    """
    Export DataFrame to single sheets in a single *.xlsx file
    """
    with pd.ExcelWriter(data_path + 'mbe_data_' + date + '.xlsx') as writer:
        for i in range(len(dataframe_list)):
            dataframe_list[i].to_excel(writer, sheet_name=dataframe_list[i].name)

    return print('file successfully exported')


def epicdf_combine(dataframe_list):
    """
    This function is used to combine the imported log DataFrames to a single DataFrame.
    """
    df = dataframe_list[0]
    for i in range(len(dataframe_list[1:])):
        df = pd.merge(df, dataframe_list[1:][i], on='Date', how='outer')

    return df


def epic_xlsx_single(date, data_path, df):
    """
    Export DataFrame as *.xlsx file. Must be used with the epicdf_combine function
    Sheet_name must be used with a string like 'epic_log_data',
    df.name does not work.
    """
    with pd.ExcelWriter(data_path + 'mbe_data_' + date + '.xlsx') as writer:
        df.to_excel(writer, sheet_name='epic_log_data')

    return print('file successfully exported')
