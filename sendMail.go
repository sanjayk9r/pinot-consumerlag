package main

import (
	"fmt"
	"net/smtp"
	"os"
)

// sendEmail sends an email to a list of recipients
func sendEmail(to []string, subject, body string) error {
	// Sender data
	from := os.Getenv("EMAIL_SENDER")
	password := os.Getenv("EMAIL_PASSWORD") // App Password for Gmail or SMTP password for other providers

	// SMTP server configuration (example for Gmail)
	smtpHost := "smtp.gmail.com"
	smtpPort := "587"

	// Receiver's email address (comma-separated list of recipients)
	receivers := to

	// Message format
	message := []byte("To: " + to[0] + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" +
		body + "\r\n")

	// Authentication
	auth := smtp.PlainAuth("", from, password, smtpHost)

	// Send email
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, receivers, message)
	if err != nil {
		return fmt.Errorf("error while sending email: %v", err)
	}
	return nil
}
