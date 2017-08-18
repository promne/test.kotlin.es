# -*- mode: ruby -*-
# vi: set ft=ruby :.

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

required_plugins = %w(vagrant-cachier)

plugins_to_install = required_plugins.select { |plugin| not Vagrant.has_plugin? plugin }
if not plugins_to_install.empty?
  puts "Installing plugins: #{plugins_to_install.join(' ')}"
  if system "vagrant plugin install #{plugins_to_install.join(' ')}"
    exec "vagrant #{ARGV.join(' ')}"
  else
    abort "Installation of one or more plugins has failed. Aborting."
  end
end


Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
	config.vm.box = "ubuntu/xenial64"
#  config.vm.box = "ubuntu/trusty64"
	config.vm.box_check_update = false  

	config.vbguest.auto_update = false		
		
	#vagrant-cachier
	config.cache.auto_detect = true
	config.cache.scope = :box	  

	nodes_count = 1
  
	(1..nodes_count).each do |i|  
		config.vm.define "couchdb-#{i}", primary: true  do |node|
	
			node.vm.hostname = "couchdb-#{i}"
			node.vm.provider :virtualbox do |vb|
			  vb.memory = 1024
			  vb.cpus = 4
			  vb.name = "couchdb-#{i}"
			end		
			
			node.vm.network :private_network, ip: "192.168.56.#{50+i}", netmask: "255.255.255.0"
				
			node.vm.provision :shell do |s|
				s.inline = <<-SCRIPT
					/vagrant/install-couchdb.sh					
				SCRIPT
			end
		end
	end	
end
