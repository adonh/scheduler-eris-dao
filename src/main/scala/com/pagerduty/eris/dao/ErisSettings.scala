package com.pagerduty.eris.dao

import com.pagerduty.metrics.{ Metrics, NullMetrics }

class ErisSettings(val metrics: Metrics = NullMetrics)
