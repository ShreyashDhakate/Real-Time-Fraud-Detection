variable "region" {
  type    = string
  default = "ap-south-1"
}
variable "vpc_id" {
  type = string
}
variable "subnet_id" {
  type        = string
  description = "Private subnet with outbound package/image access and SSM connectivity"
}
variable "ubuntu_ami_id" {
  type        = string
  description = "Verified Canonical Ubuntu 24.04 amd64 AMI for your region, with SSM Agent available"
}
variable "instance_type" {
  type    = string
  default = "m6i.xlarge"
}
