# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

null2_df = ... # Compute a Pandas dataframe to write into null2


# Write recipe outputs
null2 = dataiku.Dataset("null2")
null2.write_with_schema(null2_df)
