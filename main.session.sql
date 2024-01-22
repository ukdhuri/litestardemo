CREATE TABLE tbl_user (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    password VARCHAR(100) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('john_doe', 'john.doe@example.com', 'password123', 'John', 'Doe', '1990-01-01');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('jane_smith', 'jane.smith@example.com', 'password456', 'Jane', 'Smith', '1995-05-10');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('mike_johnson', 'mike.johnson@example.com', 'password789', 'Mike', 'Johnson', '1985-12-15');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('sarah_wilson', 'sarah.wilson@example.com', 'passwordabc', 'Sarah', 'Wilson', '1992-09-20');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('alex_brown', 'alex.brown@example.com', 'passworddef', 'Alex', 'Brown', '1988-07-05');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('emily_jones', 'emily.jones@example.com', 'passwordghi', 'Emily', 'Jones', '1998-03-25');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('david_smith', 'david.smith@example.com', 'passwordjkl', 'David', 'Smith', '1993-11-30');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('lisa_johnson', 'lisa.johnson@example.com', 'passwordmno', 'Lisa', 'Johnson', '1987-06-12');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('ryan_wilson', 'ryan.wilson@example.com', 'passwordpqr', 'Ryan', 'Wilson', '1991-04-15');

INSERT INTO tbl_user (username, email, password, first_name, last_name, date_of_birth)
VALUES ('olivia_brown', 'olivia.brown@example.com', 'passwordstu', 'Olivia', 'Brown', '1996-08-22');



select * from tbl_user;
