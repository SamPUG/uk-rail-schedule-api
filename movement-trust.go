package main

import (
	"encoding/json"
)

// TrustMessageEnvelope is used for initial parsing of TRUST messages
type TrustMessageEnvelope struct {
	Header  TrustHeader     `json:"header"`
	Body    json.RawMessage `json:"body"`
	TrainID string          // Parsed from body during unmarshal
}

// UnmarshalJSON implements custom unmarshaling to extract TrainID early
func (e *TrustMessageEnvelope) UnmarshalJSON(data []byte) error {
	// First unmarshal into temporary struct
	var temp struct {
		Header TrustHeader     `json:"header"`
		Body   json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	e.Header = temp.Header
	e.Body = temp.Body

	// Try to extract train_id from body
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(temp.Body, &bodyMap); err == nil {
		if trainID, ok := bodyMap["train_id"].(string); ok {
			e.TrainID = trainID
		}
	}

	return nil
}

// TrustHeader represents the header of a TRUST message
type TrustHeader struct {
	MsgType            string `json:"msg_type"`
	SourceDevID        string `json:"source_dev_id"`
	UserID             string `json:"user_id"`
	OriginalDataSource string `json:"original_data_source"`
	MsgQueueTimestamp  string `json:"msg_queue_timestamp"`
	SourceSystemID     string `json:"source_system_id"`
}

// Specific message types
type TrustActivationMessage struct {
	Header TrustHeader         `json:"header"`
	Body   TrustActivationBody `json:"body"`
}

type TrustCancellationMessage struct {
	Header TrustHeader           `json:"header"`
	Body   TrustCancellationBody `json:"body"`
}

type TrustMovementMessage struct {
	Header TrustHeader       `json:"header"`
	Body   TrustMovementBody `json:"body"`
}

type TrustReinstatementMessage struct {
	Header TrustHeader            `json:"header"`
	Body   TrustReinstatementBody `json:"body"`
}

type TrustChangeOfOriginMessage struct {
	Header TrustHeader             `json:"header"`
	Body   TrustChangeOfOriginBody `json:"body"`
}

type TrustChangeOfIdentityMessage struct {
	Header TrustHeader               `json:"header"`
	Body   TrustChangeOfIdentityBody `json:"body"`
}

type TrustChangeOfLocationMessage struct {
	Header TrustHeader               `json:"header"`
	Body   TrustChangeOfLocationBody `json:"body"`
}

// TrustActivationBody represents the body of an activation message (msg_type: 0001)
type TrustActivationBody struct {
	TrainID            string `json:"train_id"`
	ScheduleSource     string `json:"schedule_source"`
	TrainFileAddress   string `json:"train_file_address"`
	ScheduleEndDate    string `json:"schedule_end_date"`
	TPOriginTimestamp  string `json:"tp_origin_timestamp"`
	CreationTimestamp  string `json:"creation_timestamp"`
	TPOriginStanox     string `json:"tp_origin_stanox"`
	OriginDepTimestamp string `json:"origin_dep_timestamp"`
	TrainServiceCode   string `json:"train_service_code"`
	TOCId              string `json:"toc_id"`
	D1266RecordNumber  string `json:"d1266_record_number"`
	TrainCallType      string `json:"train_call_type"`
	TrainUID           string `json:"train_uid"`
	TrainCallMode      string `json:"train_call_mode"`
	ScheduleType       string `json:"schedule_type"`
	SchedOriginStanox  string `json:"sched_origin_stanox"`
	ScheduleWTTId      string `json:"schedule_wtt_id"`
	ScheduleStartDate  string `json:"schedule_start_date"`
}

// TrustCancellationBody represents the body of a cancellation message (msg_type: 0002)
type TrustCancellationBody struct {
	TrainFileAddress string `json:"train_file_address"`
	TrainServiceCode string `json:"train_service_code"`
	OrigLocStanox    string `json:"orig_loc_stanox"`
	TOCId            string `json:"toc_id"`
	DepTimestamp     string `json:"dep_timestamp"`
	DivisionCode     string `json:"division_code"`
	LocStanox        string `json:"loc_stanox"`
	CanxTimestamp    string `json:"canx_timestamp"`
	CanxReasonCode   string `json:"canx_reason_code"`
	TrainID          string `json:"train_id"`
	OrigLocTimestamp string `json:"orig_loc_timestamp"`
	CanxType         string `json:"canx_type"`
}

// TrustMovementBody represents the body of a movement message (msg_type: 0003)
type TrustMovementBody struct {
	TrainID              string `json:"train_id"`
	CurrentTrainID       string `json:"current_train_id"`
	OriginalLocStanox    string `json:"original_loc_stanox"`
	OriginalLocTimestamp string `json:"original_loc_timestamp"`
	EventSource          string `json:"event_source"`
	EventType            string `json:"event_type"`
	GBTTTimestamp        string `json:"gbtt_timestamp"`
	ActualTimestamp      string `json:"actual_timestamp"`
	PlannedEventType     string `json:"planned_event_type"`
	PlannedTimestamp     string `json:"planned_timestamp"`
	ReportingStanox      string `json:"reporting_stanox"`
	LocStanox            string `json:"loc_stanox"`
	NextReportStanox     string `json:"next_report_stanox"`
	NextReportRunTime    string `json:"next_report_run_time"`
	TimetableVariation   string `json:"timetable_variation"`
	VariationStatus      string `json:"variation_status"`
	AutoExpected         string `json:"auto_expected"`
	CorrectionInd        string `json:"correction_ind"`
	DelayMonitoringPoint string `json:"delay_monitoring_point"`
	TOCId                string `json:"toc_id"`
	DivisionCode         string `json:"division_code"`
	OffrouteInd          string `json:"offroute_ind"`
	DirectionInd         string `json:"direction_ind"`
	Route                string `json:"route"`
	LineInd              string `json:"line_ind"`
	Platform             string `json:"platform"`
	TrainFileAddress     string `json:"train_file_address"`
	TrainServiceCode     string `json:"train_service_code"`
	TrainTerminated      string `json:"train_terminated"`
}

// TrustReinstatementBody represents the body of a reinstatement message (msg_type: 0005)
type TrustReinstatementBody struct {
	TrainID                string `json:"train_id"`
	CurrentTrainID         string `json:"current_train_id"`
	ReinstatementTimestamp string `json:"reinstatement_timestamp"`
	DepTimestamp           string `json:"dep_timestamp"`
	LocStanox              string `json:"loc_stanox"`
	OriginalLocTimestamp   string `json:"original_loc_timestamp"`
	OriginalLocStanox      string `json:"original_loc_stanox"`
	TOCId                  string `json:"toc_id"`
	DivisionCode           string `json:"division_code"`
	TrainServiceCode       string `json:"train_service_code"`
	TrainFileAddress       string `json:"train_file_address"`
}

// TrustChangeOfOriginBody represents the body of a change of origin message (msg_type: 0006)
type TrustChangeOfOriginBody struct {
	TrainID              string `json:"train_id"`
	CurrentTrainID       string `json:"current_train_id"`
	COOTimestamp         string `json:"coo_timestamp"`
	ReasonCode           string `json:"reason_code"`
	DepTimestamp         string `json:"dep_timestamp"`
	LocStanox            string `json:"loc_stanox"`
	OriginalLocTimestamp string `json:"original_loc_timestamp"`
	OriginalLocStanox    string `json:"original_loc_stanox"`
	TOCId                string `json:"toc_id"`
	DivisionCode         string `json:"division_code"`
	TrainServiceCode     string `json:"train_service_code"`
	TrainFileAddress     string `json:"train_file_address"`
}

// TrustChangeOfIdentityBody represents the body of a change of identity message (msg_type: 0007)
type TrustChangeOfIdentityBody struct {
	TrainID          string `json:"train_id"`
	CurrentTrainID   string `json:"current_train_id"`
	RevisedTrainID   string `json:"revised_train_id"`
	EventTimestamp   string `json:"event_timestamp"`
	TrainFileAddress string `json:"train_file_address"`
	TrainServiceCode string `json:"train_service_code"`
}

// TrustChangeOfLocationBody represents the body of a change of location message (msg_type: 0008)
type TrustChangeOfLocationBody struct {
	TrainID              string `json:"train_id"`
	CurrentTrainID       string `json:"current_train_id"`
	EventTimestamp       string `json:"event_timestamp"`
	LocStanox            string `json:"loc_stanox"`
	DepTimestamp         string `json:"dep_timestamp"`
	OriginalLocStanox    string `json:"original_loc_stanox"`
	OriginalLocTimestamp string `json:"original_loc_timestamp"`
	TrainFileAddress     string `json:"train_file_address"`
	TrainServiceCode     string `json:"train_service_code"`
}

// ParseTrustMessage parses the envelope and body into the specific message type
func ParseTrustMessage(envelope *TrustMessageEnvelope) (interface{}, error) {
	switch envelope.Header.MsgType {
	case "0001":
		var msg TrustActivationMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0002":
		var msg TrustCancellationMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0003":
		var msg TrustMovementMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0005":
		var msg TrustReinstatementMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0006":
		var msg TrustChangeOfOriginMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0007":
		var msg TrustChangeOfIdentityMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	case "0008":
		var msg TrustChangeOfLocationMessage
		msg.Header = envelope.Header
		if err := json.Unmarshal(envelope.Body, &msg.Body); err != nil {
			return nil, err
		}
		return msg, nil
	default:
		// Return envelope for unknown message types
		return envelope, nil
	}
}

// GORM database models for TRUST messages

type TrustActivation struct {
	ID                 uint   `gorm:"primaryKey"`
	MessageKey         string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID            string `gorm:"index"`
	MsgQueueTimestamp  string
	TrainUID           string `gorm:"index"`
	CombinedID         string `gorm:"index"` // Links to Schedule: train_uid + schedule_start_date + schedule_type
	ScheduleSource     string
	TrainFileAddress   string
	ScheduleEndDate    string
	TPOriginTimestamp  string
	CreationTimestamp  string
	TPOriginStanox     string
	OriginDepTimestamp string
	TrainServiceCode   string
	TOCId              string
	D1266RecordNumber  string
	TrainCallType      string
	TrainCallMode      string
	ScheduleType       string
	SchedOriginStanox  string
	ScheduleWTTId      string
	ScheduleStartDate  string
}

type TrustCancellation struct {
	ID                uint   `gorm:"primaryKey"`
	MessageKey        string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID           string `gorm:"index"`
	MsgQueueTimestamp string
	TrainFileAddress  string
	TrainServiceCode  string
	OrigLocStanox     string
	TOCId             string
	DepTimestamp      string
	DivisionCode      string
	LocStanox         string
	CanxTimestamp     string
	CanxReasonCode    string
	OrigLocTimestamp  string
	CanxType          string
}

type TrustMovement struct {
	ID                   uint   `gorm:"primaryKey"`
	MessageKey           string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID              string `gorm:"index"`
	MsgQueueTimestamp    string
	CurrentTrainID       string `gorm:"index"`
	OriginalLocStanox    string
	OriginalLocTimestamp string
	EventSource          string
	EventType            string
	GBTTTimestamp        string
	ActualTimestamp      string
	PlannedEventType     string
	PlannedTimestamp     string
	ReportingStanox      string
	LocStanox            string `gorm:"index"`
	NextReportStanox     string
	NextReportRunTime    string
	TimetableVariation   string
	VariationStatus      string
	AutoExpected         string
	CorrectionInd        string
	DelayMonitoringPoint string
	TOCId                string
	DivisionCode         string
	OffrouteInd          string
	DirectionInd         string
	Route                string
	LineInd              string
	Platform             string
	TrainFileAddress     string
	TrainServiceCode     string
	TrainTerminated      string
}

type TrustReinstatement struct {
	ID                     uint   `gorm:"primaryKey"`
	MessageKey             string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID                string `gorm:"index"`
	MsgQueueTimestamp      string
	CurrentTrainID         string `gorm:"index"`
	ReinstatementTimestamp string
	DepTimestamp           string
	LocStanox              string
	OriginalLocTimestamp   string
	OriginalLocStanox      string
	TOCId                  string
	DivisionCode           string
	TrainServiceCode       string
	TrainFileAddress       string
}

type TrustChangeOfOrigin struct {
	ID                   uint   `gorm:"primaryKey"`
	MessageKey           string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID              string `gorm:"index"`
	MsgQueueTimestamp    string
	CurrentTrainID       string `gorm:"index"`
	COOTimestamp         string
	ReasonCode           string
	DepTimestamp         string
	LocStanox            string
	OriginalLocTimestamp string
	OriginalLocStanox    string
	TOCId                string
	DivisionCode         string
	TrainServiceCode     string
	TrainFileAddress     string
}

type TrustChangeOfIdentity struct {
	ID                uint   `gorm:"primaryKey"`
	MessageKey        string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID           string `gorm:"index"`
	MsgQueueTimestamp string
	CurrentTrainID    string `gorm:"index"`
	RevisedTrainID    string `gorm:"index"`
	EventTimestamp    string
	TrainFileAddress  string
	TrainServiceCode  string
}

type TrustChangeOfLocation struct {
	ID                   uint   `gorm:"primaryKey"`
	MessageKey           string `gorm:"index"` // train_id + msg_queue_timestamp for duplicate detection
	TrainID              string `gorm:"index"`
	MsgQueueTimestamp    string
	CurrentTrainID       string `gorm:"index"`
	EventTimestamp       string
	LocStanox            string
	DepTimestamp         string
	OriginalLocStanox    string
	OriginalLocTimestamp string
	TrainFileAddress     string
	TrainServiceCode     string
}

// Conversion methods to create database models from message types

func (m *TrustActivationMessage) ToTrustActivation() TrustActivation {
	// Fix TRUST bug: schedule_type has O and P swapped
	// O should be P (Permanent) and P should be O (Overlay)
	correctedScheduleType := m.Body.ScheduleType
	switch m.Body.ScheduleType {
	case "O":
		correctedScheduleType = "P"
	case "P":
		correctedScheduleType = "O"
	}

	return TrustActivation{
		MessageKey:         m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:            m.Body.TrainID,
		MsgQueueTimestamp:  m.Header.MsgQueueTimestamp,
		TrainUID:           m.Body.TrainUID,
		CombinedID:         m.Body.TrainUID + m.Body.ScheduleStartDate + correctedScheduleType,
		ScheduleSource:     m.Body.ScheduleSource,
		TrainFileAddress:   m.Body.TrainFileAddress,
		ScheduleEndDate:    m.Body.ScheduleEndDate,
		TPOriginTimestamp:  m.Body.TPOriginTimestamp,
		CreationTimestamp:  m.Body.CreationTimestamp,
		TPOriginStanox:     m.Body.TPOriginStanox,
		OriginDepTimestamp: m.Body.OriginDepTimestamp,
		TrainServiceCode:   m.Body.TrainServiceCode,
		TOCId:              m.Body.TOCId,
		D1266RecordNumber:  m.Body.D1266RecordNumber,
		TrainCallType:      m.Body.TrainCallType,
		TrainCallMode:      m.Body.TrainCallMode,
		ScheduleType:       correctedScheduleType,
		SchedOriginStanox:  m.Body.SchedOriginStanox,
		ScheduleWTTId:      m.Body.ScheduleWTTId,
		ScheduleStartDate:  m.Body.ScheduleStartDate,
	}
}

func (m *TrustCancellationMessage) ToTrustCancellation() TrustCancellation {
	return TrustCancellation{
		MessageKey:        m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:           m.Body.TrainID,
		MsgQueueTimestamp: m.Header.MsgQueueTimestamp,
		TrainFileAddress:  m.Body.TrainFileAddress,
		TrainServiceCode:  m.Body.TrainServiceCode,
		OrigLocStanox:     m.Body.OrigLocStanox,
		TOCId:             m.Body.TOCId,
		DepTimestamp:      m.Body.DepTimestamp,
		DivisionCode:      m.Body.DivisionCode,
		LocStanox:         m.Body.LocStanox,
		CanxTimestamp:     m.Body.CanxTimestamp,
		CanxReasonCode:    m.Body.CanxReasonCode,
		OrigLocTimestamp:  m.Body.OrigLocTimestamp,
		CanxType:          m.Body.CanxType,
	}
}

func (m *TrustMovementMessage) ToTrustMovement() TrustMovement {
	return TrustMovement{
		MessageKey:           m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:              m.Body.TrainID,
		MsgQueueTimestamp:    m.Header.MsgQueueTimestamp,
		CurrentTrainID:       m.Body.CurrentTrainID,
		OriginalLocStanox:    m.Body.OriginalLocStanox,
		OriginalLocTimestamp: m.Body.OriginalLocTimestamp,
		EventSource:          m.Body.EventSource,
		EventType:            m.Body.EventType,
		GBTTTimestamp:        m.Body.GBTTTimestamp,
		ActualTimestamp:      m.Body.ActualTimestamp,
		PlannedEventType:     m.Body.PlannedEventType,
		PlannedTimestamp:     m.Body.PlannedTimestamp,
		ReportingStanox:      m.Body.ReportingStanox,
		LocStanox:            m.Body.LocStanox,
		NextReportStanox:     m.Body.NextReportStanox,
		NextReportRunTime:    m.Body.NextReportRunTime,
		TimetableVariation:   m.Body.TimetableVariation,
		VariationStatus:      m.Body.VariationStatus,
		AutoExpected:         m.Body.AutoExpected,
		CorrectionInd:        m.Body.CorrectionInd,
		DelayMonitoringPoint: m.Body.DelayMonitoringPoint,
		TOCId:                m.Body.TOCId,
		DivisionCode:         m.Body.DivisionCode,
		OffrouteInd:          m.Body.OffrouteInd,
		DirectionInd:         m.Body.DirectionInd,
		Route:                m.Body.Route,
		LineInd:              m.Body.LineInd,
		Platform:             m.Body.Platform,
		TrainFileAddress:     m.Body.TrainFileAddress,
		TrainServiceCode:     m.Body.TrainServiceCode,
		TrainTerminated:      m.Body.TrainTerminated,
	}
}

func (m *TrustReinstatementMessage) ToTrustReinstatement() TrustReinstatement {
	return TrustReinstatement{
		MessageKey:             m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:                m.Body.TrainID,
		MsgQueueTimestamp:      m.Header.MsgQueueTimestamp,
		CurrentTrainID:         m.Body.CurrentTrainID,
		ReinstatementTimestamp: m.Body.ReinstatementTimestamp,
		DepTimestamp:           m.Body.DepTimestamp,
		LocStanox:              m.Body.LocStanox,
		OriginalLocTimestamp:   m.Body.OriginalLocTimestamp,
		OriginalLocStanox:      m.Body.OriginalLocStanox,
		TOCId:                  m.Body.TOCId,
		DivisionCode:           m.Body.DivisionCode,
		TrainServiceCode:       m.Body.TrainServiceCode,
		TrainFileAddress:       m.Body.TrainFileAddress,
	}
}

func (m *TrustChangeOfOriginMessage) ToTrustChangeOfOrigin() TrustChangeOfOrigin {
	return TrustChangeOfOrigin{
		MessageKey:           m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:              m.Body.TrainID,
		MsgQueueTimestamp:    m.Header.MsgQueueTimestamp,
		CurrentTrainID:       m.Body.CurrentTrainID,
		COOTimestamp:         m.Body.COOTimestamp,
		ReasonCode:           m.Body.ReasonCode,
		DepTimestamp:         m.Body.DepTimestamp,
		LocStanox:            m.Body.LocStanox,
		OriginalLocTimestamp: m.Body.OriginalLocTimestamp,
		OriginalLocStanox:    m.Body.OriginalLocStanox,
		TOCId:                m.Body.TOCId,
		DivisionCode:         m.Body.DivisionCode,
		TrainServiceCode:     m.Body.TrainServiceCode,
		TrainFileAddress:     m.Body.TrainFileAddress,
	}
}

func (m *TrustChangeOfIdentityMessage) ToTrustChangeOfIdentity() TrustChangeOfIdentity {
	return TrustChangeOfIdentity{
		MessageKey:        m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:           m.Body.TrainID,
		MsgQueueTimestamp: m.Header.MsgQueueTimestamp,
		CurrentTrainID:    m.Body.CurrentTrainID,
		RevisedTrainID:    m.Body.RevisedTrainID,
		EventTimestamp:    m.Body.EventTimestamp,
		TrainFileAddress:  m.Body.TrainFileAddress,
		TrainServiceCode:  m.Body.TrainServiceCode,
	}
}

func (m *TrustChangeOfLocationMessage) ToTrustChangeOfLocation() TrustChangeOfLocation {
	return TrustChangeOfLocation{
		MessageKey:           m.Body.TrainID + "_" + m.Header.MsgQueueTimestamp,
		TrainID:              m.Body.TrainID,
		MsgQueueTimestamp:    m.Header.MsgQueueTimestamp,
		CurrentTrainID:       m.Body.CurrentTrainID,
		EventTimestamp:       m.Body.EventTimestamp,
		LocStanox:            m.Body.LocStanox,
		DepTimestamp:         m.Body.DepTimestamp,
		OriginalLocStanox:    m.Body.OriginalLocStanox,
		OriginalLocTimestamp: m.Body.OriginalLocTimestamp,
		TrainFileAddress:     m.Body.TrainFileAddress,
		TrainServiceCode:     m.Body.TrainServiceCode,
	}
}
