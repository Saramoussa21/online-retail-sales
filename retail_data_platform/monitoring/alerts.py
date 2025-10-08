
"""
Quality Alerting System
"""

from datetime import datetime
from typing import Dict, Any, List
from ..utils.logging_config import ETLLogger

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

# Global instance
quality_alert_manager = QualityAlertManager()