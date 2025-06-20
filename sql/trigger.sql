CREATE TABLE IF NOT EXISTS users_log_before(
     user_id BIGINT PRIMARY KEY,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users_log_before(
     user_id BIGINT PRIMARY KEY,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DELIMITER //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'UPDATE');
END //

DELIMITER //

CREATE TRIGGER before_insert_users
BEFORE INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'INSERT');
END //

DELIMITER //

CREATE TRIGGER before_delete_users
BEFORE DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'DELETE');
END //

DELIMITER ;


CREATE TABLE IF NOT EXISTS users_log_after(
     user_id BIGINT PRIMARY KEY,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


// DELIMITER

CREATE TRIGGER after_update_users
AFTER UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'UPDATE');
END //

// DELIMITER

CREATE TRIGGER after_insert_users
AFTER INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, 'INSERT');
END //

// DELIMITER

CREATE TRIGGER after_delete_users
AFTER DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, 'DELETE');
END //

DELIMITER;