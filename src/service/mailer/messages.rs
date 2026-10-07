use slipstream::UserId;

pub trait MessageTemplate {
	fn subject(&self) -> String;
	fn render(&self) -> String;
}

fn footer(body: &str) -> String {
	format!("{body}\n\nMessage sent by Continuwuity {}. 🐈", env!("CARGO_PKG_VERSION"))
}

pub struct ChangeEmail<'a> {
	pub server_name: &'a str,
	pub user_id: Option<&'a UserId>,
	pub verification_link: String,
}

impl MessageTemplate for ChangeEmail<'_> {
	fn subject(&self) -> String { "Verify your email address".to_owned() }

	fn render(&self) -> String {
		let account = self.user_id.map_or_else(
			|| format!("a Matrix account on {}", self.server_name),
			|user_id| format!("the Matrix account {user_id}"),
		);
		footer(&format!(
			"Hello!\n\nSomebody, probably you, tried to associate this email address with \
			 {account}.\nIf that was you, and this is your email address, click this link to \
			 proceed:\n    {}\nOtherwise, you can ignore this email. The above link will expire \
			 in one hour.",
			self.verification_link
		))
	}
}

pub struct NewAccount<'a> {
	pub server_name: &'a str,
	pub verification_link: String,
}

impl MessageTemplate for NewAccount<'_> {
	fn subject(&self) -> String { "Create your new Matrix account".to_owned() }

	fn render(&self) -> String {
		footer(&format!(
			"Hello!\n\nSomebody, probably you, tried to create a Matrix account on {} using \
			 this email address.\nUse the link below to proceed with creating your account:\n    \
			 {}\nIf you are not trying to create an account, you can ignore this email. The \
			 above link will expire in one hour.",
			self.server_name, self.verification_link
		))
	}
}

pub struct PasswordReset<'a> {
	pub display_name: Option<&'a str>,
	pub user_id: &'a UserId,
	pub verification_link: String,
}

impl MessageTemplate for PasswordReset<'_> {
	fn subject(&self) -> String { format!("Password reset request for {}", self.user_id) }

	fn render(&self) -> String {
		let greeting = self.display_name.map_or_else(
			|| format!("Hello {},", self.user_id),
			|display_name| format!("Hello {display_name} ({}),", self.user_id),
		);
		footer(&format!(
			"{greeting}\n\nSomebody, probably you, tried to reset your Matrix account's \
			 password.\nIf you requested for your password to be reset, click this link to \
			 proceed:\n    {}\nOtherwise, you can ignore this email. The above link will expire \
			 in one hour.",
			self.verification_link
		))
	}
}

pub struct Test;

impl MessageTemplate for Test {
	fn subject(&self) -> String { "Test message".to_owned() }

	fn render(&self) -> String {
		footer("If you're seeing this, SMTP is configured correctly. :3")
	}
}
