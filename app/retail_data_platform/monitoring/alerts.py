
"""
Quality Alerting System
"""

from datetime import datetime
from typing import Dict, Any, List
from ..utils.logging_config import ETLLogger
import json

class QualityAlertManager:
    def __init__(self):
        self.logger = ETLLogger(self.__class__.__name__)
    
    def check_and_alert(self, quality_metrics: Dict[str, Any], 
                       table_name: str = 'fact_sales') -> None:
        """Check quality metrics and send alerts if needed"""
        overall_score = quality_metrics.get('overall_score', 100)
        
        # Critical alert - score below 70%
        if overall_score < 70:
            self._send_alert('CRITICAL', f"Critical quality issue in {table_name}", {
                'score': overall_score,
                'failed_checks': quality_metrics.get('failed_checks', 0),
                'table': table_name
            })
        
        # Warning alert - score below 90%
        elif overall_score < 90:
            self._send_alert('WARNING', f"Quality degradation in {table_name}", {
                'score': overall_score,
                'failed_checks': quality_metrics.get('failed_checks', 0),
                'table': table_name
            })
    
    def check_anomalies(self, anomalies: List[Dict[str, Any]]) -> None:
        """Check for quality anomalies and alert"""
        high_severity = [a for a in anomalies if a.get('severity') == 'HIGH']
        
        if high_severity:
            self._send_alert('ERROR', f"Quality anomalies detected", {
                'anomaly_count': len(high_severity),
                'worst_drop': max(a['score_drop'] for a in high_severity),
                'affected_tables': list(set(a['table_name'] for a in high_severity))
            })
    
    def _send_alert(self, level: str, message: str, details: Dict[str, Any]) -> None:
        """Send alert (currently logs, can be extended to email/Slack)"""
        alert_msg = f"QUALITY ALERT [{level}] {message}: {details}"
        
        if level == 'CRITICAL':
            self.logger.critical(alert_msg)
        elif level == 'ERROR':
            self.logger.error(alert_msg)
        elif level == 'WARNING':
            self.logger.warning(alert_msg)
        else:
            self.logger.info(alert_msg)

    def send_db_alert(level: str, message: str):
        """
        Convenience: send a simple test alert. Logs at requested level and
        optionally persists into a DB table if available / configured.
        """
        from retail_data_platform.utils.logging_config import get_logger
        logger = get_logger(__name__)
        payload = {"level": level, "message": message, "source": "cli_test"}
        # log to app log (this is the simplest sink)
        if level == 'CRITICAL':
            logger.critical(message)
        elif level == 'ERROR':
            logger.error(message)
        elif level == 'WARNING':
            logger.warning(message)
        else:
            logger.info(message)

        # optional: persist alert to DB table retail_dw.data_quality_alerts if present
        try:
            from retail_data_platform.database.connection import db_manager
            sql = """
            INSERT INTO retail_dw.data_quality_alerts (alert_time, severity, message, metadata)
            VALUES (NOW(), :severity, :message, :metadata::jsonb)
            """
            params = {'severity': level, 'message': message, 'metadata': json.dumps(payload)}
            db_manager.execute_query(sql, params)
        except Exception:
            # silent fail — persistence is optional and must not break CLI
            logger.debug("Alert persistence skipped (table missing or DB error)", exc_info=True)


# Global instance
quality_alert_manager = QualityAlertManager()