use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::PgWireResult;

pub enum AuthType {
  Default {
    password: String,
  },
  Scram {
    password: String,
    key_slice: Vec<u8>,
    cert_slice: Vec<u8>,
  },
}

pub struct TrexAuthSource {
  password: String,
}

pub fn get_startup_handler(
  auth_type: &AuthType,
) -> Arc<
  Md5PasswordAuthStartupHandler<TrexAuthSource, DefaultServerParameterProvider>,
> {
  match auth_type {
    AuthType::Default { password } => {
      Arc::new(Md5PasswordAuthStartupHandler::new(
        Arc::new(TrexAuthSource {
          password: password.to_string(),
        }),
        Arc::new(DefaultServerParameterProvider::default()),
      ))
    }
    AuthType::Scram {
      password,
      key_slice: _,
      cert_slice: _,
    } => Arc::new(Md5PasswordAuthStartupHandler::new(
      Arc::new(TrexAuthSource {
        password: password.to_string(),
      }),
      Arc::new(DefaultServerParameterProvider::default()),
    )),
  }
}

#[async_trait]
impl AuthSource for TrexAuthSource {
  async fn get_password(
    &self,
    login_info: &LoginInfo,
  ) -> PgWireResult<Password> {
    //println!("login info: {:?}", login_info);

    let salt = vec![0, 0, 0, 0];
    let password = self.password.as_str();
    //let user = login_info.user().unwrap();
    //let db = login_info.database().unwrap();
    //println!("login: {user} {db}");

    let hash_password = hash_md5_password(
      login_info.user().as_ref().unwrap(),
      password,
      salt.as_ref(),
    );
    Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
  }
}
