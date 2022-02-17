
resource "null_resource" "copy_view1" {
  provisioner "local-exec" {
    command = "python Copy_table_view_sp.py"
  }
}