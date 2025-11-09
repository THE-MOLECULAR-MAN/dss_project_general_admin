# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

x_df = ... # Compute a Pandas dataframe to write into x


# Write recipe outputs
x = dataiku.Dataset("x")
x.write_with_schema(x_df)
