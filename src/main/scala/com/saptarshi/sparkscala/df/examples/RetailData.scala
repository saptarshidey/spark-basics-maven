package com.saptarshi.sparkscala.df.examples

import java.sql.Date
import java.sql.Timestamp

case class RetailData (
    tran_date: Date,
    tran_time: Timestamp,
    city: String,
    product: String,
    sales: Double,
    payment_mode: String
)
